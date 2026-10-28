//! Native gRPC (tonic) service implementations — thin adapters over the shared handlers, so
//! native gRPC (HTTP/2, GRPC_PORT) and gRPC-Web (proxy.rs) behave identically.

use tonic::{Request, Response, Status};

use super::{crashlytics, messaging, performance, remote_config, AppState, RequestMeta};
use crate::pb_dm::crashlytics_service_server::CrashlyticsService;
use crate::pb_dm::messaging_service_server::MessagingService;
use crate::pb_dm::performance_service_server::PerformanceService;
use crate::pb_dm::remote_config_service_server::RemoteConfigService;
use crate::pb_dm::{
    Ack, FetchConfigRequest, FetchConfigResponse, RegisterDeviceRequest, RegisterDeviceResponse,
    UnregisterDeviceRequest, UpdateTopicSubscriptionRequest, UploadCrashReportsRequest, UploadPerformanceRequest,
};

pub struct DmSvc(pub AppState);

fn split<T>(request: Request<T>) -> (RequestMeta, T) {
    (RequestMeta::from_metadata(request.metadata()), request.into_inner())
}

#[tonic::async_trait]
impl CrashlyticsService for DmSvc {
    async fn upload_crash_reports(&self, request: Request<UploadCrashReportsRequest>) -> Result<Response<Ack>, Status> {
        let (meta, message) = split(request);
        crashlytics::upload_crash_reports(&self.0, &meta, message).await.map(Response::new)
    }
}

#[tonic::async_trait]
impl RemoteConfigService for DmSvc {
    async fn fetch_config(&self, request: Request<FetchConfigRequest>) -> Result<Response<FetchConfigResponse>, Status> {
        let (meta, message) = split(request);
        remote_config::fetch_config(&self.0, &meta, message).await.map(Response::new)
    }
}

#[tonic::async_trait]
impl PerformanceService for DmSvc {
    async fn upload_performance(&self, request: Request<UploadPerformanceRequest>) -> Result<Response<Ack>, Status> {
        let (meta, message) = split(request);
        performance::upload_performance(&self.0, &meta, message).await.map(Response::new)
    }
}

#[tonic::async_trait]
impl MessagingService for DmSvc {
    async fn register_device(&self, request: Request<RegisterDeviceRequest>) -> Result<Response<RegisterDeviceResponse>, Status> {
        let (meta, message) = split(request);
        messaging::register_device(&self.0, &meta, message).await.map(Response::new)
    }

    async fn update_topic_subscription(&self, request: Request<UpdateTopicSubscriptionRequest>) -> Result<Response<Ack>, Status> {
        let (meta, message) = split(request);
        messaging::update_topic_subscription(&self.0, &meta, message).await.map(Response::new)
    }

    async fn unregister_device(&self, request: Request<UnregisterDeviceRequest>) -> Result<Response<Ack>, Status> {
        let (meta, message) = split(request);
        messaging::unregister_device(&self.0, &meta, message).await.map(Response::new)
    }
}
