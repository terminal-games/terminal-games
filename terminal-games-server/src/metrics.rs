// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::net::IpAddr;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

use anyhow::{Context, Result};
use prometheus::{
    Encoder, GaugeVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder, core::Collector,
};
use sysinfo::{CpuRefreshKind, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use terminal_games::app::{SessionAppState, SessionEndReason};

use crate::cluster_kicked_ips;
use crate::sessions::SessionRegistry;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Transport {
    Ssh,
    Web,
}

impl Transport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ssh => "ssh",
            Self::Web => "web",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthKind {
    Anonymous,
    Authenticated,
}

impl AuthKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Anonymous => "false",
            Self::Authenticated => "true",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    In,
    Out,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
        }
    }
}

struct DurationRecord {
    user_id: Option<u64>,
    shortname: String,
    seconds: f64,
}

#[derive(Clone)]
pub struct SessionHandle {
    live_session: Arc<LiveSession>,
}

impl SessionHandle {
    pub fn set_active_app(&self, app: &SessionAppState) {
        self.live_session.set_active_shortname(&app.shortname);
    }

    pub fn record_bytes(&self, direction: Direction, bytes: usize) {
        self.live_session.record_bytes(direction, bytes);
    }

    pub fn finish(&self, reason: SessionEndReason) {
        self.live_session.finish(reason);
    }
}

struct ActiveSegment {
    shortname: String,
    started_at: Instant,
}

struct LiveSession {
    metrics: Arc<ServerMetrics>,
    user_id: Option<u64>,
    transport: Transport,
    authenticated: AuthKind,
    has_audio: bool,
    current: Mutex<Option<ActiveSegment>>,
    finished: AtomicBool,
}

impl LiveSession {
    fn new(
        metrics: Arc<ServerMetrics>,
        app: SessionAppState,
        user_id: Option<u64>,
        transport: Transport,
        authenticated: AuthKind,
        has_audio: bool,
    ) -> Arc<Self> {
        let session = Arc::new(Self {
            metrics,
            user_id,
            transport,
            authenticated,
            has_audio,
            current: Mutex::new(None),
            finished: AtomicBool::new(false),
        });
        session.set_active_shortname(&app.shortname);
        session
    }

    fn segment_labels<'a>(&'a self, shortname: &'a str) -> [&'a str; 5] {
        [
            self.metrics.region.as_str(),
            shortname,
            self.transport.as_str(),
            self.authenticated.as_str(),
            bool_label(self.has_audio),
        ]
    }

    fn close_current(&self, guard: &mut Option<ActiveSegment>) {
        let Some(segment) = guard.take() else {
            return;
        };
        self.metrics
            .active_sessions
            .with_label_values(&self.segment_labels(&segment.shortname))
            .dec();
        let elapsed = segment.started_at.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        self.metrics
            .record_persisted_duration(self.user_id, &segment.shortname, elapsed);
    }

    fn set_active_shortname(&self, shortname: &str) {
        let mut guard = match self.current.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if guard
            .as_ref()
            .is_some_and(|segment| segment.shortname == shortname)
        {
            return;
        }

        self.close_current(&mut guard);

        self.metrics
            .active_sessions
            .with_label_values(&self.segment_labels(shortname))
            .inc();
        self.metrics
            .sessions_total
            .with_label_values(&self.segment_labels(shortname))
            .inc();

        *guard = Some(ActiveSegment {
            shortname: shortname.to_string(),
            started_at: Instant::now(),
        });
    }

    fn record_bytes(&self, direction: Direction, bytes: usize) {
        self.metrics
            .bytes_total
            .with_label_values(&[
                direction.as_str(),
                self.metrics.region.as_str(),
                self.transport.as_str(),
            ])
            .inc_by(bytes as u64);
    }

    fn finish(&self, reason: SessionEndReason) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        self.metrics
            .session_ends_total
            .with_label_values(&[
                self.metrics.region.as_str(),
                self.transport.as_str(),
                self.authenticated.as_str(),
                bool_label(self.has_audio),
                reason.slug(),
            ])
            .inc();
    }
}

impl Drop for LiveSession {
    fn drop(&mut self) {
        let mut guard = match self.current.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.close_current(&mut guard);
    }
}

pub struct ServerMetrics {
    registry: Registry,
    region: String,
    db: libsql::Connection,
    duration_writer: tokio::sync::mpsc::UnboundedSender<DurationRecord>,
    cluster_kicked_ip_writer: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,

    admission_waiting_sessions: IntGaugeVec,
    admission_queue_exits_total: IntCounterVec,
    ip_ban_events_total: IntCounterVec,
    ip_bans_active: IntGaugeVec,
    cluster_enforcement_total: IntCounterVec,
    cluster_kicked_ip_count: GaugeVec,
    active_sessions: IntGaugeVec,
    live_session_duration_seconds: GaugeVec,
    sessions_total: IntCounterVec,
    session_ends_total: IntCounterVec,
    session_duration_seconds: GaugeVec,
    bytes_total: IntCounterVec,
    process_resident_memory_bytes: IntGaugeVec,
    process_virtual_memory_bytes: IntGaugeVec,
    system_cpu_count: IntGaugeVec,
    system_memory_total_bytes: IntGaugeVec,
    system_memory_available_bytes: IntGaugeVec,
    system_memory_used_bytes: IntGaugeVec,
    system_load_average: GaugeVec,
    session_registry: Mutex<Option<Weak<SessionRegistry>>>,
}

impl ServerMetrics {
    pub async fn new(admission_max_running: usize, db: libsql::Connection) -> Result<Arc<Self>> {
        let registry = Registry::new();
        let region = std::env::var("REGION_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "loca".to_string());

        let build_info = register(
            &registry,
            IntGaugeVec::new(
                Opts::new("terminal_games_build_info", "Static build metadata"),
                &["service", "version", "region"],
            )?,
        )?;
        build_info
            .with_label_values(&["terminal-games-server", env!("CARGO_PKG_VERSION"), &region])
            .set(1);

        let admission_max_running_metric = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_admission_max_running",
                    "Configured concurrent session limit",
                ),
                &["region"],
            )?,
        )?;
        admission_max_running_metric
            .with_label_values(&[&region])
            .set(if admission_max_running == usize::MAX {
                0
            } else {
                admission_max_running as i64
            });

        let admission_waiting_sessions = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_admission_waiting_sessions",
                    "Sessions waiting for admission grouped by region and transport",
                ),
                &["region", "transport"],
            )?,
        )?;
        let admission_queue_exits_total = register(
            &registry,
            IntCounterVec::new(
                Opts::new(
                    "terminal_games_admission_queue_exits_total",
                    "Queued sessions that left the queue, grouped by outcome",
                ),
                &["region", "transport", "outcome"],
            )?,
        )?;
        let ip_ban_events_total = register(
            &registry,
            IntCounterVec::new(
                Opts::new(
                    "terminal_games_ip_ban_events_total",
                    "IP ban lifecycle events grouped by outcome",
                ),
                &["region", "outcome"],
            )?,
        )?;
        let ip_bans_active = register(
            &registry,
            IntGaugeVec::new(
                Opts::new("terminal_games_ip_bans_active", "Currently active IP bans"),
                &["region"],
            )?,
        )?;
        let cluster_enforcement_total = register(
            &registry,
            IntCounterVec::new(
                Opts::new(
                    "terminal_games_cluster_enforcement_total",
                    "Cluster-defense enforcement actions grouped by transport and action",
                ),
                &["region", "transport"],
            )?,
        )?;
        let cluster_kicked_ip_count = register(
            &registry,
            GaugeVec::new(
                Opts::new(
                    "terminal_games_cluster_kicked_ip_count",
                    "Current cluster-kicked counts for normalized IPs that are not covered by an active ban",
                ),
                &["region", "ip"],
            )?,
        )?;
        let active_sessions = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_active_sessions",
                    "Currently active shortname session segments grouped by session attributes",
                ),
                &[
                    "region",
                    "shortname",
                    "transport",
                    "authenticated",
                    "has_audio",
                ],
            )?,
        )?;
        let live_session_duration_seconds = register(
            &registry,
            GaugeVec::new(
                Opts::new(
                    "terminal_games_live_session_duration_seconds",
                    "Current duration of each live session grouped by stable session identifiers",
                ),
                &["region", "session_id", "transport", "shortname"],
            )?,
        )?;
        let sessions_total = register(
            &registry,
            IntCounterVec::new(
                Opts::new(
                    "terminal_games_sessions_total",
                    "Total shortname session segments started, grouped by session attributes",
                ),
                &[
                    "region",
                    "shortname",
                    "transport",
                    "authenticated",
                    "has_audio",
                ],
            )?,
        )?;
        let session_ends_total = register(
            &registry,
            IntCounterVec::new(
                Opts::new(
                    "terminal_games_session_ends_total",
                    "Completed sessions grouped by session attributes and close reason",
                ),
                &[
                    "region",
                    "transport",
                    "authenticated",
                    "has_audio",
                    "reason",
                ],
            )?,
        )?;
        let session_duration_seconds = register(
            &registry,
            GaugeVec::new(
                Opts::new(
                    "terminal_games_session_duration_seconds",
                    "Total persisted time spent across all users for each shortname",
                ),
                &["region", "shortname"],
            )?,
        )?;

        let bytes_total = register(
            &registry,
            IntCounterVec::new(
                Opts::new(
                    "terminal_games_bytes_total",
                    "Network bytes grouped by direction, region, and transport",
                ),
                &["direction", "region", "transport"],
            )?,
        )?;

        let process_resident_memory_bytes = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_process_resident_memory_bytes",
                    "Resident set size used by the terminal-games-server process",
                ),
                &["region"],
            )?,
        )?;

        let process_virtual_memory_bytes = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_process_virtual_memory_bytes",
                    "Virtual memory size used by the terminal-games-server process",
                ),
                &["region"],
            )?,
        )?;

        let system_cpu_count = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_system_cpu_count",
                    "Logical CPU count visible to the node",
                ),
                &["region"],
            )?,
        )?;

        let system_memory_total_bytes = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_system_memory_total_bytes",
                    "Total system memory visible to the node",
                ),
                &["region"],
            )?,
        )?;

        let system_memory_available_bytes = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_system_memory_available_bytes",
                    "Available system memory visible to the node",
                ),
                &["region"],
            )?,
        )?;

        let system_memory_used_bytes = register(
            &registry,
            IntGaugeVec::new(
                Opts::new(
                    "terminal_games_system_memory_used_bytes",
                    "Used system memory derived from total minus available memory",
                ),
                &["region"],
            )?,
        )?;

        let system_load_average = register(
            &registry,
            GaugeVec::new(
                Opts::new(
                    "terminal_games_system_load_average",
                    "System load average across the node",
                ),
                &["region", "window"],
            )?,
        )?;

        let (duration_writer, duration_rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_duration_writer(db.clone(), duration_rx);
        let (cluster_kicked_ip_writer, cluster_kicked_ip_rx) =
            tokio::sync::mpsc::unbounded_channel();
        spawn_cluster_kicked_ip_writer(db.clone(), cluster_kicked_ip_rx);

        Ok(Arc::new(Self {
            registry,
            region,
            db,
            duration_writer,
            cluster_kicked_ip_writer,
            admission_waiting_sessions,
            admission_queue_exits_total,
            ip_ban_events_total,
            ip_bans_active,
            cluster_enforcement_total,
            cluster_kicked_ip_count,
            active_sessions,
            live_session_duration_seconds,
            sessions_total,
            session_ends_total,
            session_duration_seconds,
            bytes_total,
            process_resident_memory_bytes,
            process_virtual_memory_bytes,
            system_cpu_count,
            system_memory_total_bytes,
            system_memory_available_bytes,
            system_memory_used_bytes,
            system_load_average,
            session_registry: Mutex::new(None),
        }))
    }

    pub fn set_session_registry(&self, session_registry: &Arc<SessionRegistry>) {
        *self.session_registry.lock().unwrap() = Some(Arc::downgrade(session_registry));
    }

    pub async fn render(&self) -> Result<String> {
        self.update_system_metrics();
        self.refresh_persisted_shortname_durations().await?;
        self.refresh_cluster_kicked_ip_metrics().await?;
        self.refresh_live_session_metrics();

        let families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder
            .encode(&families, &mut buffer)
            .context("failed to encode prometheus metrics")?;
        String::from_utf8(buffer).context("prometheus metrics were not valid UTF-8")
    }

    pub fn record_admission_state(&self, queued_ssh: usize, queued_web: usize) {
        self.admission_waiting_sessions
            .with_label_values(&[self.region.as_str(), Transport::Ssh.as_str()])
            .set(queued_ssh as i64);
        self.admission_waiting_sessions
            .with_label_values(&[self.region.as_str(), Transport::Web.as_str()])
            .set(queued_web as i64);
    }

    pub fn record_admission_joined_from_queue(&self, transport: Transport) {
        self.admission_queue_exits_total
            .with_label_values(&[self.region.as_str(), transport.as_str(), "joined"])
            .inc();
    }

    pub fn record_admission_abandoned_queue(&self, transport: Transport) {
        self.admission_queue_exits_total
            .with_label_values(&[self.region.as_str(), transport.as_str(), "abandoned"])
            .inc();
    }

    pub fn record_ip_ban_update(
        &self,
        activated: usize,
        deactivated: usize,
        evicted_from_queue: usize,
        active_ban_count: usize,
    ) {
        if activated > 0 {
            self.ip_ban_events_total
                .with_label_values(&[self.region.as_str(), "activated"])
                .inc_by(activated as u64);
        }
        if deactivated > 0 {
            self.ip_ban_events_total
                .with_label_values(&[self.region.as_str(), "deactivated"])
                .inc_by(deactivated as u64);
        }
        if evicted_from_queue > 0 {
            self.ip_ban_events_total
                .with_label_values(&[self.region.as_str(), "queue_evicted"])
                .inc_by(evicted_from_queue as u64);
        }
        self.ip_bans_active
            .with_label_values(&[self.region.as_str()])
            .set(active_ban_count as i64);
    }

    pub fn record_cluster_enforcement(&self, transport: Transport) {
        self.cluster_enforcement_total
            .with_label_values(&[self.region.as_str(), transport.as_str()])
            .inc();
    }

    pub fn record_cluster_kicked_ip(&self, ip: IpAddr) {
        let ip = cluster_kicked_ips::normalize(ip);
        let display_ip =
            cluster_kicked_ips::display(&ip).unwrap_or_else(|_| format!("{:02x?}", ip.as_slice()));
        if let Err(error) = self.cluster_kicked_ip_writer.send(ip) {
            tracing::error!(?error, ip = %display_ip, "failed to queue cluster-kicked ip persistence");
        }
    }

    pub fn record_bytes(&self, direction: Direction, transport: Transport, bytes: usize) {
        self.bytes_total
            .with_label_values(&[direction.as_str(), self.region.as_str(), transport.as_str()])
            .inc_by(bytes as u64);
    }

    pub fn start_session(
        self: &Arc<Self>,
        app: SessionAppState,
        transport: Transport,
        authenticated: AuthKind,
        has_audio: bool,
        user_id: Option<u64>,
    ) -> SessionHandle {
        let session = LiveSession::new(
            self.clone(),
            app,
            user_id,
            transport,
            authenticated,
            has_audio,
        );
        SessionHandle {
            live_session: session,
        }
    }

    fn record_persisted_duration(&self, user_id: Option<u64>, shortname: &str, seconds: f64) {
        if seconds <= 0.0 {
            return;
        }
        if let Err(error) = self.duration_writer.send(DurationRecord {
            user_id,
            shortname: shortname.to_string(),
            seconds,
        }) {
            tracing::error!(
                ?error,
                shortname,
                seconds,
                "failed to queue duration persistence"
            );
        }
    }

    fn update_system_metrics(&self) {
        let mut system = System::new();
        system.refresh_memory();
        system.refresh_cpu_list(CpuRefreshKind::nothing());

        let pid = Pid::from_u32(std::process::id());
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_memory().without_tasks(),
        );
        let (process_resident_memory_bytes, process_virtual_memory_bytes) = system
            .process(pid)
            .map(|process| (process.memory(), process.virtual_memory()))
            .unwrap_or((0, 0));

        let total_memory = system.total_memory();
        let available_memory = system.available_memory();

        self.process_resident_memory_bytes
            .with_label_values(&[self.region.as_str()])
            .set(process_resident_memory_bytes as i64);
        self.process_virtual_memory_bytes
            .with_label_values(&[self.region.as_str()])
            .set(process_virtual_memory_bytes as i64);
        self.system_cpu_count
            .with_label_values(&[self.region.as_str()])
            .set(system.cpus().len() as i64);
        self.system_memory_total_bytes
            .with_label_values(&[self.region.as_str()])
            .set(total_memory as i64);
        self.system_memory_available_bytes
            .with_label_values(&[self.region.as_str()])
            .set(available_memory as i64);
        self.system_memory_used_bytes
            .with_label_values(&[self.region.as_str()])
            .set(total_memory.saturating_sub(available_memory) as i64);

        let loads = System::load_average();
        self.system_load_average
            .with_label_values(&[self.region.as_str(), "1m"])
            .set(loads.one);
        self.system_load_average
            .with_label_values(&[self.region.as_str(), "5m"])
            .set(loads.five);
        self.system_load_average
            .with_label_values(&[self.region.as_str(), "15m"])
            .set(loads.fifteen);
    }

    async fn refresh_persisted_shortname_durations(&self) -> Result<()> {
        self.session_duration_seconds.reset();
        for (shortname, seconds) in load_persisted_shortname_durations(&self.db).await? {
            self.session_duration_seconds
                .with_label_values(&[self.region.as_str(), shortname.as_str()])
                .set(seconds);
        }
        Ok(())
    }

    async fn refresh_cluster_kicked_ip_metrics(&self) -> Result<()> {
        self.cluster_kicked_ip_count.reset();
        for entry in cluster_kicked_ips::load_visible_page(&self.db, 0, 20, true)
            .await?
            .entries
        {
            self.cluster_kicked_ip_count
                .with_label_values(&[self.region.as_str(), entry.ip.as_str()])
                .set(entry.count as f64);
        }
        Ok(())
    }

    fn refresh_live_session_metrics(&self) {
        self.live_session_duration_seconds.reset();
        let session_registry = self
            .session_registry
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade);
        let Some(session_registry) = session_registry else {
            return;
        };
        for session in session_registry.summaries() {
            self.live_session_duration_seconds
                .with_label_values(&[
                    self.region.as_str(),
                    session.session_id.as_str(),
                    session.transport.as_str(),
                    session.shortname.as_str(),
                ])
                .set(session.duration_seconds as f64);
        }
    }
}

fn spawn_duration_writer(
    db: libsql::Connection,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<DurationRecord>,
) {
    tokio::spawn(async move {
        while let Some(record) = rx.recv().await {
            match persist_duration_record(&db, &record).await {
                Ok(()) => {}
                Err(error) => {
                    tracing::error!(
                        error = ?error,
                        shortname = %record.shortname,
                        seconds = record.seconds,
                        user_id = record.user_id,
                        "failed to persist duration record"
                    );
                }
            }
        }
    });
}

fn spawn_cluster_kicked_ip_writer(
    db: libsql::Connection,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
) {
    tokio::spawn(async move {
        while let Some(ip) = rx.recv().await {
            let display_ip = cluster_kicked_ips::display(&ip)
                .unwrap_or_else(|_| format!("{:02x?}", ip.as_slice()));
            match cluster_kicked_ips::increment(&db, &ip).await {
                Ok(()) => {}
                Err(error) => {
                    tracing::error!(
                        error = ?error,
                        ip = %display_ip,
                        "failed to persist cluster-kicked ip record"
                    );
                }
            }
        }
    });
}

async fn persist_duration_record(db: &libsql::Connection, record: &DurationRecord) -> Result<()> {
    let tx = db
        .transaction()
        .await
        .context("failed to start duration transaction")?;

    let affected = tx
        .execute(
            "UPDATE apps
             SET duration_seconds = duration_seconds + ?2
             WHERE shortname = ?1",
            libsql::params!(record.shortname.as_str(), record.seconds),
        )
        .await
        .context("failed to update global app duration")?;
    if affected == 0 {
        tracing::warn!(shortname = %record.shortname, "no app row found while persisting duration");
    }

    if let Some(user_id) = record.user_id {
        let user_affected = tx
            .execute(
                "UPDATE users
                 SET session_time = session_time + ?2
                 WHERE id = ?1",
                libsql::params!(user_id, record.seconds),
            )
            .await
            .context("failed to update user session time")?;
        if user_affected == 0 {
            tracing::warn!(user_id, "no user row found while persisting session time");
        }

        tx.execute(
            "INSERT INTO user_app_durations (user_id, app_id, duration_seconds)
             SELECT ?1, id, ?3 FROM apps WHERE shortname = ?2
             ON CONFLICT(user_id, app_id) DO UPDATE
             SET duration_seconds = user_app_durations.duration_seconds + excluded.duration_seconds",
            libsql::params!(user_id, record.shortname.as_str(), record.seconds),
        )
        .await
        .context("failed to upsert user app duration")?;
    }

    tx.commit()
        .await
        .context("failed to commit duration transaction")?;
    Ok(())
}

async fn load_persisted_shortname_durations(db: &libsql::Connection) -> Result<Vec<(String, f64)>> {
    let mut rows = db
        .query(
            "SELECT shortname, CAST(duration_seconds AS REAL)
             FROM apps
             WHERE duration_seconds > 0",
            (),
        )
        .await
        .context("failed to load persisted app durations")?;

    let mut durations = Vec::new();
    while let Some(row) = rows.next().await.context("failed to read duration row")? {
        let shortname = row.get::<String>(0).context("missing duration shortname")?;
        let seconds = row.get::<f64>(1).context("missing duration seconds")?;
        durations.push((shortname, seconds));
    }
    Ok(durations)
}

fn bool_label(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn register<T>(registry: &Registry, metric: T) -> Result<T>
where
    T: Collector + Clone + 'static,
{
    registry
        .register(Box::new(metric.clone()))
        .context("failed to register prometheus collector")?;
    Ok(metric)
}
