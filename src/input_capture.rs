#![allow(dead_code, unused_variables)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use futures::TryStreamExt;
use tokio::sync::mpsc::Sender;
use zbus::zvariant::{self, OwnedObjectPath, OwnedValue};

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
    /// The compositor's session ID (returned from org.cosmic.InputCapture.CreateSession)
    compositor_session_id: Option<String>,
}

impl SessionData {
    fn close(&mut self) {
        self.closed = true;
    }
}

/// Reverse mapping from compositor session ID to portal session handle (ObjectPath).
/// Shared between the main struct and the signal relay task.
type SessionMap = Arc<Mutex<HashMap<String, OwnedObjectPath>>>;

// Main InputCapture struct
pub struct InputCapture {
    tx: Sender<subscription::Event>,
    /// Dedicated D-Bus connection for calling the compositor's InputCapture service.
    /// We cannot reuse the injected zbus connection because calling another service
    /// from within a D-Bus method handler on the same connection deadlocks.
    compositor_conn: tokio::sync::OnceCell<zbus::Connection>,
    /// Maps compositor session_id -> portal session_handle ObjectPath.
    session_map: SessionMap,
    /// Ensures the signal relay background task is spawned only once.
    relay_started: AtomicBool,
}

impl InputCapture {
    pub fn new(tx: Sender<subscription::Event>) -> Self {
        Self {
            tx,
            compositor_conn: tokio::sync::OnceCell::new(),
            session_map: Arc::new(Mutex::new(HashMap::new())),
            relay_started: AtomicBool::new(false),
        }
    }

    async fn comp_conn(&self) -> Option<&zbus::Connection> {
        self.compositor_conn
            .get_or_try_init(|| async {
                tracing::warn!("InputCapture: Creating dedicated compositor D-Bus connection");
                zbus::Connection::session().await
            })
            .await
            .map_err(|e| tracing::error!("InputCapture: Failed to create compositor connection: {}", e))
            .ok()
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
        tracing::info!("InputCapture: CreateSession from {} (parent: {})", app_id, parent_window);

        // Create a session on the compositor first to get its session ID.
        // Use a dedicated connection — the injected one deadlocks on cross-service calls.
        let compositor_session_id = match self.comp_conn().await {
            Some(comp) => match comp.call_method(
                Some("org.cosmic.InputCapture"),
                "/org/cosmic/InputCapture",
                Some("org.cosmic.InputCapture"),
                "CreateSession",
                &(CAPABILITY_KEYBOARD | CAPABILITY_POINTER,),
            ).await {
            Ok(reply) => {
                match reply.body().deserialize::<String>() {
                    Ok(id) => {
                        tracing::warn!("InputCapture: Compositor session ID: {}", id);
                        Some(id)
                    }
                    Err(e) => {
                        tracing::warn!("InputCapture: Failed to get compositor session ID: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                tracing::warn!("InputCapture: Compositor CreateSession failed: {}", e);
                None
            }
            },
            None => None,
        };

        // Store reverse mapping: compositor session_id -> portal session_handle
        if let Some(ref comp_sid) = compositor_session_id {
            self.session_map
                .lock()
                .unwrap()
                .insert(comp_sid.clone(), session_handle.to_owned().into());
        }

        let session_data = SessionData {
            state: Some(SessionState::Created),
            compositor_session_id,
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

        // Start the compositor signal relay task (once).
        if !self.relay_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            if let Some(comp_conn) = self.comp_conn().await.cloned() {
                let portal_conn = connection.clone();
                let session_map = Arc::clone(&self.session_map);
                tokio::spawn(async move {
                    signal_relay_loop(comp_conn, portal_conn, session_map).await;
                });
            }
        }

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
                    tracing::warn!(
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
            tracing::info!("InputCapture: Start approved for {}", app_id);

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

        // Get compositor session ID
        let comp_sid = interface.get().await.compositor_session_id.clone().unwrap_or_default();

        // Get zones from compositor via private D-Bus interface
        let Some(comp) = self.comp_conn().await else {
            tracing::error!("InputCapture: No compositor connection for GetZones");
            return PortalResponse::Other;
        };
        let reply = comp.call_method(
            Some("org.cosmic.InputCapture"),
            "/org/cosmic/InputCapture",
            Some("org.cosmic.InputCapture"),
            "GetZones",
            &(comp_sid.as_str(),),
        ).await;
        let reply = match reply {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("InputCapture: Compositor GetZones failed: {}", e);
                return PortalResponse::Other;
            }
        };
        let (zone_set, zones) = match reply.body().deserialize::<(u32, Vec<(u32, u32, i32, i32)>)>() {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("InputCapture: Failed to deserialize zones: {}", e);
                return PortalResponse::Other;
            }
        };
        tracing::info!("InputCapture: GetZones: zone_set={}, {} zones", zone_set, zones.len());

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
            tracing::warn!(
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
                        tracing::warn!("InputCapture: Barrier {} is not axis-aligned", barrier_id);
                        failed_barriers.push(barrier_id);
                        continue;
                    }
                    // Validate: must not be a point
                    if x1 == x2 && y1 == y2 {
                        tracing::warn!("InputCapture: Barrier {} is a point", barrier_id);
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

        session_data.barriers = valid_barriers.clone();

        // Forward barriers to compositor using its session ID
        let sid = session_data.compositor_session_id.clone().unwrap_or_default();
        let compositor_barriers: Vec<(u32, (i32, i32, i32, i32))> = valid_barriers
            .iter()
            .map(|b| (b.id, (b.x1, b.y1, b.x2, b.y2)))
            .collect();
        drop(session_data);
        { 
            if let Some(comp) = self.comp_conn().await { let _ = comp.call_method(
                Some("org.cosmic.InputCapture"),
                "/org/cosmic/InputCapture",
                Some("org.cosmic.InputCapture"),
                "SetBarriers",
                &(sid.as_str(), zone_set, compositor_barriers),
            ).await.map_err(|e| tracing::warn!("Compositor SetBarriers failed: {}", e)); }
        }

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
            Some(SessionState::Disabled) | Some(SessionState::Started) | Some(SessionState::Created) => {
                session_data.state = Some(SessionState::Enabled);
                tracing::info!("InputCapture: Enabled for session {}", session_handle);
                // Forward to compositor using its session ID
                let sid = session_data.compositor_session_id.clone().unwrap_or_default();
                drop(session_data);
                { 
                    if let Some(comp) = self.comp_conn().await { let _ = comp.call_method(
                        Some("org.cosmic.InputCapture"),
                        "/org/cosmic/InputCapture",
                        Some("org.cosmic.InputCapture"),
                        "Enable",
                        &(sid.as_str(),),
                    ).await.map_err(|e| tracing::warn!("Compositor Enable failed: {}", e)); }
                }
                Ok(())
            }
            _ => {
                tracing::warn!(
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
                tracing::info!("InputCapture: Disabled for session {}", session_handle);
                let sid = session_data.compositor_session_id.clone().unwrap_or_default();
                drop(session_data);
                { 
                    if let Some(comp) = self.comp_conn().await { let _ = comp.call_method(
                        Some("org.cosmic.InputCapture"),
                        "/org/cosmic/InputCapture",
                        Some("org.cosmic.InputCapture"),
                        "Disable",
                        &(sid.as_str(),),
                    ).await.map_err(|e| tracing::warn!("Compositor Disable failed: {}", e)); }
                }
                Ok(())
            }
            _ => {
                tracing::warn!(
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
        let activation_id = options
            .get("activation_id")
            .and_then(|v| <u32>::try_from(v).ok())
            .unwrap_or(0);
        let cursor_position = options
            .get("cursor_position")
            .and_then(|v| <(f64, f64)>::try_from(v.clone()).ok())
            .unwrap_or((0.0, 0.0));

        session_data.state = Some(SessionState::Disabled);
        tracing::info!(
            "InputCapture: Released for session {} (activation_id={}, cursor=({},{}))",
            session_handle, activation_id, cursor_position.0, cursor_position.1
        );

        // Forward Release to compositor so it stops capturing and warps cursor
        let sid = session_data.compositor_session_id.clone().unwrap_or_default();
        drop(session_data);
        { 
            if let Some(comp) = self.comp_conn().await { let _ = comp.call_method(
                Some("org.cosmic.InputCapture"),
                "/org/cosmic/InputCapture",
                Some("org.cosmic.InputCapture"),
                "Release",
                &(sid.as_str(), activation_id, cursor_position),
            ).await.map_err(|e| tracing::warn!("Compositor Release failed: {}", e)); }
        }
        Ok(())
    }

    // ConnectToEIS - returns Unix fd for EIS connection
    //
    // The compositor (cosmic-comp) runs the EIS server. We ask it to create
    // a socketpair via its private D-Bus interface. The compositor keeps the
    // server end (for sending input events) and returns the client end, which
    // we pass back to the xdg-desktop-portal frontend for Deskflow.
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

        // Get compositor session ID
        let comp_sid = {
            let data = _interface.get().await;
            data.compositor_session_id.clone().unwrap_or_default()
        };
        tracing::info!("InputCapture: ConnectToEIS for session {} (compositor: {})", session_handle, comp_sid);

        // Ask the compositor to create the socketpair and EIS server context.
        // It keeps the server end and returns the client end.
        let comp = self.comp_conn().await
            .ok_or_else(|| zbus::fdo::Error::Failed("No compositor connection".into()))?;
        let reply = comp
            .call_method(
                Some("org.cosmic.InputCapture"),
                "/org/cosmic/InputCapture",
                Some("org.cosmic.InputCapture"),
                "ConnectToEIS",
                &(comp_sid.as_str(),),
            )
            .await
            .map_err(|e| zbus::fdo::Error::Failed(format!("Compositor ConnectToEIS: {}", e)))?;

        let client_fd: zbus::zvariant::OwnedFd = reply.body().deserialize()
            .map_err(|e| zbus::fdo::Error::Failed(format!("fd deserialize: {}", e)))?;

        tracing::info!("InputCapture: Got EIS client fd from compositor for session {}", session_handle);

        Ok(client_fd)
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

/// Background task: subscribes to compositor `org.cosmic.InputCapture` signals
/// and re-emits them on the portal backend interface so that xdg-desktop-portal
/// can relay them to clients (e.g. Deskflow).
async fn signal_relay_loop(
    comp_conn: zbus::Connection,
    portal_conn: zbus::Connection,
    session_map: SessionMap,
) {
    let rule = zbus::MatchRule::builder()
        .msg_type(zbus::message::Type::Signal)
        .sender("org.cosmic.InputCapture")
        .unwrap()
        .path("/org/cosmic/InputCapture")
        .unwrap()
        .interface("org.cosmic.InputCapture")
        .unwrap()
        .build();

    let mut stream = match zbus::MessageStream::for_match_rule(rule, &comp_conn, None).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("InputCapture: Failed to subscribe to compositor signals: {}", e);
            return;
        }
    };

    tracing::warn!("InputCapture: Signal relay task started");

    loop {
        let msg = match stream.try_next().await {
            Ok(Some(m)) => m,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("InputCapture: Signal stream error: {}", e);
                continue;
            }
        };

        let member = msg.header().member().map(|m| m.as_str().to_string());

        match member.as_deref() {
            Some("Activated") => {
                let body = match msg
                    .body()
                    .deserialize::<(String, u32, u32, (f64, f64))>()
                {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("InputCapture: Failed to deserialize Activated: {}", e);
                        continue;
                    }
                };
                let (session_id, barrier_id, activation_id, cursor_position) = body;

                let portal_handle = session_map.lock().unwrap().get(&session_id).cloned();
                let Some(portal_handle) = portal_handle else {
                    tracing::warn!(
                        "InputCapture: No portal session for compositor session_id={}",
                        session_id
                    );
                    continue;
                };

                let mut options: HashMap<String, OwnedValue> = HashMap::new();
                options.insert(
                    "activation_id".to_string(),
                    zvariant::Value::from(activation_id).try_to_owned().unwrap(),
                );
                options.insert(
                    "cursor_position".to_string(),
                    zvariant::Value::from((cursor_position.0, cursor_position.1))
                        .try_to_owned()
                        .unwrap(),
                );
                if barrier_id != 0 {
                    options.insert(
                        "barrier_id".to_string(),
                        zvariant::Value::from(barrier_id).try_to_owned().unwrap(),
                    );
                }

                tracing::warn!(
                    "InputCapture: Relaying Activated for session {} (activation_id={}, barrier_id={}, cursor=({},{}))",
                    session_id, activation_id, barrier_id, cursor_position.0, cursor_position.1
                );

                emit_portal_signal(&portal_conn, portal_handle.as_ref(), "Activated", &options)
                    .await;
            }
            Some("Deactivated") => {
                let body = match msg.body().deserialize::<(String, u32)>() {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("InputCapture: Failed to deserialize Deactivated: {}", e);
                        continue;
                    }
                };
                let (session_id, activation_id) = body;

                let portal_handle = session_map.lock().unwrap().get(&session_id).cloned();
                let Some(portal_handle) = portal_handle else {
                    tracing::warn!(
                        "InputCapture: No portal session for compositor session_id={}",
                        session_id
                    );
                    continue;
                };

                let mut options: HashMap<String, OwnedValue> = HashMap::new();
                options.insert(
                    "activation_id".to_string(),
                    zvariant::Value::from(activation_id).try_to_owned().unwrap(),
                );

                tracing::warn!(
                    "InputCapture: Relaying Deactivated for session {} (activation_id={})",
                    session_id, activation_id
                );

                emit_portal_signal(&portal_conn, portal_handle.as_ref(), "Deactivated", &options)
                    .await;
            }
            Some("DisabledSignal") => {
                let body = match msg.body().deserialize::<(String,)>() {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("InputCapture: Failed to deserialize DisabledSignal: {}", e);
                        continue;
                    }
                };
                let (session_id,) = body;

                let portal_handle = session_map.lock().unwrap().get(&session_id).cloned();
                let Some(portal_handle) = portal_handle else {
                    tracing::warn!(
                        "InputCapture: No portal session for compositor session_id={}",
                        session_id
                    );
                    continue;
                };

                let options: HashMap<String, OwnedValue> = HashMap::new();

                tracing::warn!(
                    "InputCapture: Relaying Disabled for session {}",
                    session_id
                );

                emit_portal_signal(&portal_conn, portal_handle.as_ref(), "Disabled", &options)
                    .await;
            }
            _ => {}
        }
    }

    tracing::warn!("InputCapture: Signal relay task ended");
}

/// Emit a signal on the portal's `org.freedesktop.impl.portal.InputCapture` interface
/// using the portal connection's object server and the generated signal methods.
async fn emit_portal_signal(
    portal_conn: &zbus::Connection,
    session_handle: zvariant::ObjectPath<'_>,
    signal_name: &str,
    options: &HashMap<String, OwnedValue>,
) {
    let iface_ref = portal_conn
        .object_server()
        .interface::<_, InputCapture>("/org/freedesktop/portal/desktop")
        .await;

    match iface_ref {
        Ok(iface_ref) => {
            let ctxt = iface_ref.signal_emitter();
            let iface = iface_ref.get().await;
            let result = match signal_name {
                "Activated" => {
                    iface
                        .activated(ctxt, session_handle, options.clone())
                        .await
                }
                "Deactivated" => {
                    iface
                        .deactivated(ctxt, session_handle, options.clone())
                        .await
                }
                "Disabled" => {
                    iface
                        .disabled(ctxt, session_handle, options.clone())
                        .await
                }
                other => {
                    tracing::warn!("InputCapture: Unknown signal to relay: {}", other);
                    return;
                }
            };
            if let Err(e) = result {
                tracing::warn!("InputCapture: Failed to emit {} signal: {}", signal_name, e);
            }
        }
        Err(e) => {
            tracing::warn!(
                "InputCapture: Could not get interface ref for signal emission: {}",
                e
            );
        }
    }
}
