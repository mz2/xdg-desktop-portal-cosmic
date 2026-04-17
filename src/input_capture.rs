#![allow(dead_code, unused_variables)]

use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use zbus::zvariant;

use crate::{PortalResponse, Request, subscription};

const CAPABILITY_KEYBOARD: u32 = 1;
const CAPABILITY_POINTER: u32 = 2;
const CAPABILITY_TOUCHSCREEN: u32 = 4;

#[derive(zvariant::SerializeDict, zvariant::Type)]
#[zvariant(signature = "a{sv}")]
struct CreateSessionResult {
    session_id: String,
    capabilities: u32,
}

// Zone: (width, height, x_offset, y_offset)
#[derive(zvariant::SerializeDict, zvariant::Type)]
#[zvariant(signature = "a{sv}")]
struct GetZonesResult {
    zones: Vec<(u32, u32, i32, i32)>,
    zone_set: u32,
}

#[derive(zvariant::SerializeDict, zvariant::Type)]
#[zvariant(signature = "a{sv}")]
struct SetPointerBarriersResult {
    failed_barriers: Vec<u32>,
}

#[derive(zvariant::DeserializeDict, zvariant::Type)]
#[zvariant(signature = "a{sv}")]
struct StartOptions {
    capabilities: Option<u32>,
    persist_mode: Option<u32>,
}

#[derive(zvariant::SerializeDict, zvariant::Type)]
#[zvariant(signature = "a{sv}")]
struct StartResult {
    capabilities: u32,
}

// Barrier: axis-aligned line segment
#[derive(Debug, Clone)]
struct Barrier {
    id: u32,
    x1: i32,
    y1: i32,
    x2: i32,
    y2: i32,
}

// Session state machine
#[derive(Debug, Clone, Copy, PartialEq)]
enum SessionState {
    Created,
    Started,
    Enabled,
    Disabled,
    Activated,
}

// Per-session data
#[derive(Default)]
struct SessionData {
    state: Option<SessionState>,
    capabilities: u32,
    barriers: Vec<Barrier>,
    zone_set: u32,
    closed: bool,
}

impl SessionData {
    fn close(&mut self) {
        self.closed = true;
    }
}

// Main InputCapture struct
pub struct InputCapture {
    tx: Sender<subscription::Event>,
}

impl InputCapture {
    pub fn new(tx: Sender<subscription::Event>) -> Self {
        Self { tx }
    }
}

#[zbus::interface(name = "org.freedesktop.impl.portal.InputCapture")]
impl InputCapture {
    // CreateSession method (called by xdg-desktop-portal frontend)
    // Signature: (o handle, o session_handle, s app_id, s parent_window, a{sv} options)
    async fn create_session(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        handle: zvariant::ObjectPath<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        parent_window: String,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> PortalResponse<CreateSessionResult> {
        log::info!("InputCapture: CreateSession from {} (parent: {})", app_id, parent_window);
        let session_data = SessionData {
            state: Some(SessionState::Created),
            ..Default::default()
        };
        connection
            .object_server()
            .at(
                &session_handle,
                crate::Session::new(session_data, |session_data| session_data.close()),
            )
            .await
            .unwrap();
        PortalResponse::Success(CreateSessionResult {
            session_id: session_handle.to_string(),
            capabilities: CAPABILITY_KEYBOARD | CAPABILITY_POINTER,
        })
    }

    // Start method - shows permission dialog, transitions to Started
    async fn start(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        handle: zvariant::ObjectPath<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        parent_window: String,
        options: StartOptions,
    ) -> PortalResponse<StartResult> {
        let on_cancel = || async {};
        Request::run(connection, &handle, on_cancel, async {
            let Some(interface) =
                crate::session_interface::<SessionData>(connection, &session_handle).await
            else {
                return PortalResponse::Other;
            };

            {
                let mut session_data = interface.get_mut().await;
                if session_data.state != Some(SessionState::Created) {
                    log::warn!(
                        "InputCapture: Start called in invalid state: {:?}",
                        session_data.state
                    );
                    return PortalResponse::Other;
                }
                let requested_caps = options
                    .capabilities
                    .unwrap_or(CAPABILITY_KEYBOARD | CAPABILITY_POINTER);
                let granted_caps = requested_caps & (CAPABILITY_KEYBOARD | CAPABILITY_POINTER);
                session_data.capabilities = granted_caps;
                session_data.state = Some(SessionState::Disabled);
            }

            // TODO: Show permission dialog via self.tx channel
            // For now, auto-approve (we'll add the dialog later)
            log::info!("InputCapture: Start approved for {}", app_id);

            let caps = interface.get().await.capabilities;
            PortalResponse::Success(StartResult {
                capabilities: caps,
            })
        })
        .await
    }

    // GetZones - returns output layout as zones
    async fn get_zones(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        handle: zvariant::ObjectPath<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> PortalResponse<GetZonesResult> {
        let Some(interface) =
            crate::session_interface::<SessionData>(connection, &session_handle).await
        else {
            return PortalResponse::Other;
        };

        // TODO: Get real zones from compositor via private D-Bus interface
        // For now, return a placeholder single zone
        let zones = vec![(1920, 1080, 0, 0)];
        let zone_set = 1;

        interface.get_mut().await.zone_set = zone_set;

        PortalResponse::Success(GetZonesResult { zones, zone_set })
    }

    // SetPointerBarriers - validates and stores barrier geometry
    async fn set_pointer_barriers(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        handle: zvariant::ObjectPath<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        options: HashMap<String, zvariant::OwnedValue>,
        barriers: Vec<HashMap<String, zvariant::OwnedValue>>,
        zone_set: u32,
    ) -> PortalResponse<SetPointerBarriersResult> {
        let Some(interface) =
            crate::session_interface::<SessionData>(connection, &session_handle).await
        else {
            return PortalResponse::Other;
        };

        let mut session_data = interface.get_mut().await;

        // Check zone_set matches
        if zone_set != session_data.zone_set {
            log::warn!(
                "InputCapture: SetPointerBarriers zone_set mismatch: {} != {}",
                zone_set,
                session_data.zone_set
            );
            return PortalResponse::Other;
        }

        let mut valid_barriers = Vec::new();
        let mut failed_barriers = Vec::new();

        for barrier_dict in &barriers {
            let barrier_id = barrier_dict
                .get("barrier_id")
                .and_then(|v| <u32>::try_from(v).ok())
                .unwrap_or(0);

            let position = barrier_dict
                .get("position")
                .and_then(|v| <(i32, i32, i32, i32)>::try_from(v.clone()).ok());

            if barrier_id == 0 {
                continue;
            }

            match position {
                Some((x1, y1, x2, y2)) => {
                    // Validate: must be axis-aligned
                    if x1 != x2 && y1 != y2 {
                        log::warn!("InputCapture: Barrier {} is not axis-aligned", barrier_id);
                        failed_barriers.push(barrier_id);
                        continue;
                    }
                    // Validate: must not be a point
                    if x1 == x2 && y1 == y2 {
                        log::warn!("InputCapture: Barrier {} is a point", barrier_id);
                        failed_barriers.push(barrier_id);
                        continue;
                    }

                    valid_barriers.push(Barrier {
                        id: barrier_id,
                        x1,
                        y1,
                        x2,
                        y2,
                    });
                }
                None => {
                    failed_barriers.push(barrier_id);
                }
            }
        }

        session_data.barriers = valid_barriers;

        PortalResponse::Success(SetPointerBarriersResult { failed_barriers })
    }

    // Enable - arm capture
    async fn enable(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::fdo::Result<()> {
        let Some(interface) =
            crate::session_interface::<SessionData>(connection, &session_handle).await
        else {
            return Err(zbus::fdo::Error::Failed("Session not found".into()));
        };

        let mut session_data = interface.get_mut().await;
        match session_data.state {
            Some(SessionState::Disabled) | Some(SessionState::Started) => {
                session_data.state = Some(SessionState::Enabled);
                log::info!("InputCapture: Enabled for session {}", session_handle);
                // TODO: Forward Enable to compositor via private D-Bus
                Ok(())
            }
            _ => {
                log::warn!(
                    "InputCapture: Enable called in invalid state: {:?}",
                    session_data.state
                );
                Err(zbus::fdo::Error::Failed("Invalid session state".into()))
            }
        }
    }

    // Disable - disarm capture
    async fn disable(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::fdo::Result<()> {
        let Some(interface) =
            crate::session_interface::<SessionData>(connection, &session_handle).await
        else {
            return Err(zbus::fdo::Error::Failed("Session not found".into()));
        };

        let mut session_data = interface.get_mut().await;
        match session_data.state {
            Some(SessionState::Enabled) => {
                session_data.state = Some(SessionState::Disabled);
                log::info!("InputCapture: Disabled for session {}", session_handle);
                // TODO: Forward Disable to compositor via private D-Bus
                Ok(())
            }
            _ => {
                log::warn!(
                    "InputCapture: Disable called in invalid state: {:?}",
                    session_data.state
                );
                Err(zbus::fdo::Error::Failed("Invalid session state".into()))
            }
        }
    }

    // Release - end capture, warp cursor
    async fn release(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::fdo::Result<()> {
        let Some(interface) =
            crate::session_interface::<SessionData>(connection, &session_handle).await
        else {
            return Err(zbus::fdo::Error::Failed("Session not found".into()));
        };

        let mut session_data = interface.get_mut().await;

        // Extract activation_id and cursor_position from options
        let _activation_id = options
            .get("activation_id")
            .and_then(|v| <u32>::try_from(v).ok());
        let _cursor_position = options
            .get("cursor_position")
            .and_then(|v| <(f64, f64)>::try_from(v.clone()).ok());

        session_data.state = Some(SessionState::Disabled);
        log::info!("InputCapture: Released for session {}", session_handle);
        // TODO: Forward Release to compositor with cursor_position via private D-Bus
        Ok(())
    }

    // ConnectToEIS - returns Unix fd for EIS connection
    #[zbus(name = "ConnectToEIS")]
    async fn connect_to_eis(
        &self,
        #[zbus(connection)] connection: &zbus::Connection,
        session_handle: zvariant::ObjectPath<'_>,
        app_id: String,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::fdo::Result<zbus::zvariant::OwnedFd> {
        let Some(_interface) =
            crate::session_interface::<SessionData>(connection, &session_handle).await
        else {
            return Err(zbus::fdo::Error::Failed("Session not found".into()));
        };

        // Create Unix socketpair
        let (client_stream, _server_stream) = std::os::unix::net::UnixStream::pair()
            .map_err(|e| zbus::fdo::Error::Failed(format!("Failed to create socketpair: {}", e)))?;

        // TODO: Pass server_stream to compositor for EIS server context via private D-Bus

        log::info!("InputCapture: ConnectToEIS for session {}", session_handle);

        use std::os::unix::io::{FromRawFd, IntoRawFd};
        // SAFETY: client_stream.into_raw_fd() yields a valid, owned fd
        let owned_fd = unsafe { std::os::unix::io::OwnedFd::from_raw_fd(client_stream.into_raw_fd()) };
        Ok(zbus::zvariant::OwnedFd::from(owned_fd))
    }

    // Signals
    #[zbus(signal)]
    async fn activated(
        &self,
        signal_ctxt: &zbus::object_server::SignalEmitter<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn deactivated(
        &self,
        signal_ctxt: &zbus::object_server::SignalEmitter<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn disabled(
        &self,
        signal_ctxt: &zbus::object_server::SignalEmitter<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn zones_changed(
        &self,
        signal_ctxt: &zbus::object_server::SignalEmitter<'_>,
        session_handle: zvariant::ObjectPath<'_>,
        options: HashMap<String, zvariant::OwnedValue>,
    ) -> zbus::Result<()>;

    // Properties
    #[zbus(property)]
    fn supported_capabilities(&self) -> u32 {
        CAPABILITY_KEYBOARD | CAPABILITY_POINTER
    }

    #[zbus(property, name = "version")]
    fn version(&self) -> u32 {
        2
    }
}
