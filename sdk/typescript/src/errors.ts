export class KetebeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class TransportError extends KetebeError {
  constructor(message: string, readonly cause?: unknown) {
    super(message);
  }
}

export class ApiError extends KetebeError {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}
