use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{
    AppState, CollectionService, JobResult, JobService, JobState, PendingRecord, RuntimeCatalog,
    WriteService,
};
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-backup-jobs-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn wait_terminal(service: &JobService, id: ketebe_server::JobId) -> ketebe_server::JobRecord {
    for _ in 0..200 {
        let record = service.get(id).unwrap();
        if matches!(
            record.state,
            JobState::Completed | JobState::Failed | JobState::Cancelled
        ) {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("job did not reach terminal state");
}

#[tokio::test]
async fn backup_and_restore_jobs_publish_progress_result_and_failure_state() {
    let dir = temp_dir("roundtrip");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1);
    let id = CollectionId::new("docs").unwrap();
    let writes = WriteService::new(state.clone());
    writes
        .create_collection(id.clone(), 2, DistanceMetric::Cosine, Vec::new())
        .await
        .unwrap();
    writes
        .upsert(
            &id,
            PendingRecord {
                id: RecordId::string("a").unwrap(),
                vector: vec![1.0, 0.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();

    let jobs = JobService::new(state.clone());
    let backup = jobs.submit_backup_create(id.clone()).unwrap();
    let backup = wait_terminal(&jobs, backup.id).await;
    assert_eq!(backup.state, JobState::Completed);
    assert_eq!(backup.progress.completed, 1);
    assert_eq!(backup.progress.total, Some(1));
    let backup_id = match backup.result.unwrap() {
        JobResult::BackupCreated { backup_id } => backup_id,
        other => panic!("unexpected backup result: {other:?}"),
    };
    assert!(
        dir.join("backups")
            .join(&backup_id)
            .join("manifest.json")
            .exists()
    );

    CollectionService::new(state.clone())
        .delete(&id)
        .await
        .unwrap();
    let restore = jobs.submit_backup_restore(backup_id.clone()).unwrap();
    let restore = wait_terminal(&jobs, restore.id).await;
    assert_eq!(restore.state, JobState::Completed);
    assert_eq!(restore.progress.completed, 1);
    assert!(matches!(
        restore.result,
        Some(JobResult::BackupRestored { backup_id: ref restored, ref collection_id })
            if restored == &backup_id && collection_id == "docs"
    ));
    assert!(dir.join("collections/docs/collection.json").exists());

    let missing = jobs.submit_backup_restore("missing-backup").unwrap();
    let missing = wait_terminal(&jobs, missing.id).await;
    assert_eq!(missing.state, JobState::Failed);
    assert_eq!(missing.error.unwrap().code, "backup_restore_failed");
    fs::remove_dir_all(dir).unwrap();
}
