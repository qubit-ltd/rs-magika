// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_operation::AsyncCleanup;
use qubit_operation::CleanupContext;
use qubit_operation::CleanupFailure;
use qubit_operation::CleanupFuture;
use qubit_operation::CleanupOutcome;
use qubit_operation::CleanupStage;
use qubit_operation::DiagnosticSummary;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::RecoveryPhase;
use qubit_operation::ResourceId;
use qubit_operation::ResourceScopeError;
use qubit_operation::ResourceScopeOptions;
use qubit_operation::ScopeLimits;

struct FailingCleanup {
    resource_id: Arc<OnceLock<ResourceId>>,
    attempts: Arc<AtomicUsize>,
    failures_before_release: usize,
}

impl AsyncCleanup<String> for FailingCleanup {
    fn cleanup(self: Arc<Self>, resource: String, _context: CleanupContext) -> CleanupFuture<String> {
        assert_eq!(resource, "retained payload");
        let resource_id = *self.resource_id.get().expect("resource ID is set before scope exit");
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if attempt < self.failures_before_release {
                CleanupOutcome::Failed {
                    failure: CleanupFailure {
                        resource_id,
                        stage: CleanupStage::AsyncRelease,
                        summary: DiagnosticSummary::new("cleanup not confirmed"),
                        source: None,
                    },
                    resource,
                }
            } else {
                CleanupOutcome::Released
            }
        })
    }
}

fn register_failing_resource(scope: &qubit_operation::OperationScope, resource_id: Arc<OnceLock<ResourceId>>) {
    let cleanup = Arc::new(FailingCleanup {
        resource_id: Arc::clone(&resource_id),
        attempts: Arc::new(AtomicUsize::new(0)),
        failures_before_release: 1,
    });
    let resource = scope
        .reserve_resource()
        .unwrap()
        .commit_with(String::from("retained payload"), cleanup)
        .unwrap_or_else(|_| panic!("reserved resource should commit"));
    resource_id
        .set(resource.id())
        .unwrap_or_else(|_| panic!("resource ID should be written once"));
    drop(resource);
}

#[test]
fn test_locked_operation_api_keeps_failed_cleanup_in_parent_report_until_release_evidence() {
    let resource_id = Arc::new(OnceLock::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let resource_id_for_operation = Arc::clone(&resource_id);
    let attempts_for_operation = Arc::clone(&attempts);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut handle = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let cleanup = Arc::new(FailingCleanup {
                resource_id: Arc::clone(&resource_id_for_operation),
                attempts: Arc::clone(&attempts_for_operation),
                failures_before_release: 2,
            });
            let resource = scope
                .reserve_resource()
                .unwrap()
                .commit_with(String::from("retained payload"), cleanup)
                .unwrap_or_else(|_| panic!("reserved resource should commit"));
            resource_id_for_operation
                .set(resource.id())
                .unwrap_or_else(|_| panic!("resource ID should be written once"));
            drop(resource);
            Ok::<_, ()>(())
        })
        .unwrap();

    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let settlement = handle.wait_settlement().unwrap();
    assert!(!settlement.all_released());
    assert_eq!(settlement.recovery_count, 1);
    assert_eq!(settlement.recoveries.len(), 1);
    assert_eq!(settlement.cleanup_failures_total, 1);
    assert_eq!(settlement.cleanup_failures[0].resource_id, *resource_id.get().unwrap());

    let recovery = settlement.recoveries[0].clone();
    assert_eq!(recovery.snapshot().phase, RecoveryPhase::Pending);
    assert_eq!(recovery.snapshot().failures_total, 0);
    let repeated_failure = recovery.try_retry().unwrap().wait().unwrap();
    assert_eq!(repeated_failure.recovery.phase, RecoveryPhase::Pending);
    assert_eq!(recovery.snapshot().failures_total, 1);

    let confirmed_release = recovery.try_retry().unwrap().wait().unwrap();
    assert_eq!(confirmed_release.recovery.phase, RecoveryPhase::Resolved);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(!settlement.all_released());

    drop(
        manager
            .request_shutdown(qubit_operation::ShutdownMode::Drain)
            .wait()
            .unwrap(),
    );
}

#[test]
fn test_exhausted_recovery_store_capacity_leaves_parent_report_unreleased() {
    use qubit_operation::RecoveryStore;
    use qubit_operation::RecoveryStoreOptions;
    use qubit_operation::RecoveryTransferError;

    let first_resource_id = Arc::new(OnceLock::new());
    let second_resource_id = Arc::new(OnceLock::new());
    let first_id_for_operation = Arc::clone(&first_resource_id);
    let second_id_for_operation = Arc::clone(&second_resource_id);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut handle = manager
        .try_submit(OperationOptions::default(), move |scope| {
            register_failing_resource(&scope, first_id_for_operation);
            register_failing_resource(&scope, second_id_for_operation);
            Ok::<_, ()>(())
        })
        .unwrap();

    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let settlement = handle.wait_settlement().unwrap();
    assert!(!settlement.all_released());
    assert_eq!(settlement.recovery_count, 2);
    assert_eq!(settlement.recoveries.len(), 2);

    let first_id = *first_resource_id.get().unwrap();
    let second_id = *second_resource_id.get().unwrap();
    let first_recovery = settlement
        .recoveries
        .iter()
        .find(|recovery| recovery.snapshot().resources[0] == first_id)
        .expect("first resource recovery should be reported")
        .clone();
    let second_recovery = settlement
        .recoveries
        .iter()
        .find(|recovery| recovery.snapshot().resources[0] == second_id)
        .expect("second resource recovery should be reported")
        .clone();
    let store = RecoveryStore::new(RecoveryStoreOptions {
        group_capacity: 1,
        resource_capacity: 1,
        ..RecoveryStoreOptions::default()
    })
    .unwrap();
    first_recovery.transfer_to(&store).unwrap();
    assert!(matches!(
        second_recovery.transfer_to(&store),
        Err(RecoveryTransferError::CapacityExceeded)
    ));
    assert_eq!(second_recovery.snapshot().phase, RecoveryPhase::Pending);
    assert!(!settlement.all_released());

    let stored_release = store.try_retry(first_recovery.id()).unwrap().wait().unwrap();
    assert_eq!(stored_release.recovery.phase, RecoveryPhase::Resolved);
    let retained_release = second_recovery.try_retry().unwrap().wait().unwrap();
    assert_eq!(retained_release.recovery.phase, RecoveryPhase::Resolved);
    assert!(!settlement.all_released());

    store.request_shutdown().wait().unwrap();
    drop(
        manager
            .request_shutdown(qubit_operation::ShutdownMode::Drain)
            .wait()
            .unwrap(),
    );
}

#[test]
fn test_locked_operation_api_reuses_resource_and_attached_scope_capacity_after_cleanup() {
    use std::time::Duration;
    use std::time::Instant;

    use qubit_operation::ResourceCapacityError;

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_operation = Arc::clone(&attempts);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut handle = manager
        .try_submit(
            OperationOptions {
                limits: ScopeLimits {
                    resources: 1,
                    resource_scopes: 1,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            },
            move |scope| {
                let first_scope = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
                assert!(matches!(
                    scope.resource_scope(ResourceScopeOptions { capacity: 1 }),
                    Err(ResourceScopeError::CapacityExceeded)
                ));

                let resource_id = Arc::new(OnceLock::new());
                let cleanup = Arc::new(FailingCleanup {
                    resource_id: Arc::clone(&resource_id),
                    attempts: Arc::clone(&attempts_for_operation),
                    failures_before_release: 0,
                });
                let resource = first_scope
                    .reserve_resource()
                    .unwrap()
                    .commit_with(String::from("retained payload"), cleanup)
                    .unwrap_or_else(|_| panic!("reserved resource should commit"));
                resource_id
                    .set(resource.id())
                    .unwrap_or_else(|_| panic!("resource ID should be written once"));
                drop(resource);
                first_scope.end();

                // Scope capacity is returned only after its cleanup report is
                // aggregated into the parent. Retry admission until that
                // public behavior shows the single attached-scope slot free.
                let deadline = Instant::now() + Duration::from_secs(5);
                let second_scope = loop {
                    match scope.resource_scope(ResourceScopeOptions { capacity: 1 }) {
                        Ok(resource_scope) => break resource_scope,
                        Err(ResourceScopeError::CapacityExceeded) if Instant::now() < deadline => {
                            std::thread::yield_now();
                        }
                        Err(error) => panic!("attached-scope capacity was not reusable: {error:?}"),
                    }
                };

                // The first resource's successful cleanup also has to return
                // the sole inherited resource permit.
                let reservation = loop {
                    match second_scope.reserve_resource() {
                        Ok(reservation) => break reservation,
                        Err(ResourceCapacityError::CapacityExceeded) if Instant::now() < deadline => {
                            std::thread::yield_now();
                        }
                        Err(error) => panic!("resource capacity was not reusable: {error:?}"),
                    }
                };
                let second_resource_id = Arc::new(OnceLock::new());
                let second_cleanup = Arc::new(FailingCleanup {
                    resource_id: Arc::clone(&second_resource_id),
                    attempts: Arc::clone(&attempts_for_operation),
                    failures_before_release: 0,
                });
                let second_resource = reservation
                    .commit_with(String::from("retained payload"), second_cleanup)
                    .unwrap_or_else(|_| panic!("reused resource reservation should commit"));
                second_resource_id
                    .set(second_resource.id())
                    .unwrap_or_else(|_| panic!("second resource ID should be written once"));
                drop(second_resource);
                second_scope.end();
                Ok::<_, ()>(())
            },
        )
        .unwrap();

    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let settlement = handle.wait_settlement().unwrap();
    assert!(settlement.all_released());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    drop(
        manager
            .request_shutdown(qubit_operation::ShutdownMode::Drain)
            .wait()
            .unwrap(),
    );
}
