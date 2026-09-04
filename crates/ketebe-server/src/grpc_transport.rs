use crate::{AppState, AuthenticationService, TransportTlsConfig};
use rustls::ServerConfig;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

#[derive(Debug)]
pub enum GrpcTransportError {
    Tls(crate::TransportTlsError),
    Io(std::io::Error),
    Grpc(tonic::transport::Error),
    Join(tokio::task::JoinError),
}

impl fmt::Display for GrpcTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tls(error) => write!(formatter, "gRPC TLS configuration failed: {error}"),
            Self::Io(error) => write!(formatter, "gRPC transport I/O failed: {error}"),
            Self::Grpc(error) => write!(formatter, "gRPC server failed: {error}"),
            Self::Join(error) => write!(formatter, "gRPC transport task failed: {error}"),
        }
    }
}

impl std::error::Error for GrpcTransportError {}

impl From<crate::TransportTlsError> for GrpcTransportError {
    fn from(value: crate::TransportTlsError) -> Self {
        Self::Tls(value)
    }
}

impl From<std::io::Error> for GrpcTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub async fn serve_grpc_transport_until_shutdown(
    state: AppState,
    public_listener: TcpListener,
    authentication: AuthenticationService,
    tls: Option<TransportTlsConfig>,
) -> Result<(), GrpcTransportError> {
    let Some(tls) = tls else {
        return crate::serve_grpc_listener_until_shutdown_with_authentication(
            state,
            public_listener,
            authentication,
        )
        .await
        .map_err(GrpcTransportError::Grpc);
    };

    let internal_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let internal_address = internal_listener.local_addr()?;
    let lifecycle = state.lifecycle();
    let mut grpc = tokio::spawn(
        crate::serve_grpc_listener_until_shutdown_with_authentication(
            state,
            internal_listener,
            authentication,
        ),
    );
    let mut ingress = tokio::spawn(serve_tls_ingress(
        public_listener,
        internal_address,
        tls.rustls_server_config()?,
        lifecycle,
    ));

    tokio::select! {
        result = &mut grpc => {
            ingress.abort();
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(GrpcTransportError::Grpc(error)),
                Err(error) => Err(GrpcTransportError::Join(error)),
            }
        }
        result = &mut ingress => {
            grpc.abort();
            match result {
                Ok(result) => result,
                Err(error) => Err(GrpcTransportError::Join(error)),
            }
        }
    }
}

async fn serve_tls_ingress(
    listener: TcpListener,
    target: SocketAddr,
    tls: Arc<ServerConfig>,
    lifecycle: Arc<crate::Lifecycle>,
) -> Result<(), GrpcTransportError> {
    let acceptor = TlsAcceptor::from(tls);
    loop {
        tokio::select! {
            () = lifecycle.wait_for_draining() => return Ok(()),
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if let Err(error) = proxy_connection(acceptor, stream, target).await {
                        tracing::warn!(client.address = %peer, error = %error, "gRPC TLS connection failed");
                    }
                });
            }
        }
    }
}

async fn proxy_connection(
    acceptor: TlsAcceptor,
    stream: TcpStream,
    target: SocketAddr,
) -> Result<(), GrpcTransportError> {
    let mut client = acceptor
        .accept(stream)
        .await
        .map_err(std::io::Error::other)?;
    let mut upstream = TcpStream::connect(target).await?;
    copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}
