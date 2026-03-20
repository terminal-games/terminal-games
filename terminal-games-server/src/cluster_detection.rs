// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Passive cluster detection for admission control.
//!
//! The detector fingerprints live sessions using only host-visible terminal I/O. Each session is
//! modeled as a stochastic control process over several rolling windows. The model combines
//! event-rate and entropy measures, burst and periodicity statistics, byte-class distributions,
//! sequence n-gram simhashes, short-range autocorrelation, spectral features, and
//! output-conditioned response timing. Those independent views are fused into per-window
//! similarities, then into pairwise evidence between sessions. We also archive recent window
//! signatures from prior sessions so the detector can score replay and template reuse across time
//! rather than only comparing currently active sessions. Pairwise evidence induces a correlation
//! graph; connected components become suspicious clusters when their aggregate likelihood remains
//! strong under current server pressure. Once a correlated cluster clears the suspicion threshold,
//! enforcement evicts the whole cluster. Isolated sessions are never evicted by this logic;
//! enforcement only applies to correlated groups.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use crate::admission::{
    BURST_BUCKET_MS, CLASS_BUCKETS, GAP_BUCKETS, IDLE_TIMEOUT_MS, INPUT_WINDOW_MS,
    LOW_PRESSURE_FLOOR, MAX_RECENT_SIGNATURES, MAX_SAMPLE_BYTES, MEDIUM_WINDOW_MS,
    MIN_CLUSTER_SIZE, MIN_INPUTS_FOR_FINGERPRINT, MOUSE_MOTION_COALESCE_MS, RESPONSE_BUCKETS,
    SHORT_WINDOW_MS, SIGNATURE_RETENTION_MS,
};
use crate::admission::{InputSample, LiveSessionRecord, OutputSample};
use smallvec::SmallVec;

const PRESSURE_SATURATION_OCCUPANCY: f64 = 0.70;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NetworkPrefix {
    V4(u32),
    V6(u64),
}

#[derive(Clone)]
struct SessionAssessment {
    session_id: u64,
    client_ip: IpAddr,
    ip_prefix: NetworkPrefix,
    started_at_ms: u64,
    low_engagement: f64,
    replay_score: f64,
    replay_match_count: usize,
    replay_density: f64,
    coupling_deficit: f64,
    mouse_motion_ratio: f64,
    mouse_coord_entropy: f64,
    spectral_peakiness: f64,
    spectral_entropy_norm: f64,
    windows: Vec<WindowFingerprint>,
}

#[derive(Clone)]
struct WindowFingerprint {
    span_ms: u64,
    confidence: f64,
    event_rate_norm: f64,
    entropy_norm: f64,
    transition_entropy_norm: f64,
    ngram_innovation: f64,
    burstiness: f64,
    periodicity_60s: f64,
    spectral_peakiness: f64,
    spectral_entropy_norm: f64,
    input_output_balance: f64,
    output_coupling: f64,
    autocorr1: f64,
    autocorr2: f64,
    mouse_ratio: f64,
    mouse_motion_ratio: f64,
    mouse_coord_entropy: f64,
    mouse_flood_score: f64,
    gap_hist: [f64; GAP_BUCKETS],
    response_hist: [f64; RESPONSE_BUCKETS],
    class_hist: [f64; CLASS_BUCKETS],
    unigram_hash: u64,
    bigram_hash: u64,
    trigram_hash: u64,
    transition_hash: u64,
}

#[derive(Clone, Copy)]
struct PairEvidence {
    a: u64,
    b: u64,
    score: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClusterScores {
    pub(crate) avg_edge: f64,
    pub(crate) avg_replay: f64,
    pub(crate) avg_replay_density: f64,
    pub(crate) avg_low_engagement: f64,
    pub(crate) avg_coupling_deficit: f64,
    pub(crate) network_cohesion: f64,
    pub(crate) start_sync: f64,
    pub(crate) size_factor: f64,
    pub(crate) pressure: f64,
    pub(crate) edge_low_engagement_interaction: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClusterEvictionSummary {
    pub(crate) cluster_size: usize,
    pub(crate) score: f64,
    pub(crate) required_score: f64,
    pub(crate) score_margin: f64,
    pub(crate) factors: ClusterScores,
    pub(crate) contributions: ClusterScores,
}

pub(crate) struct ClusterEvaluation {
    pub(crate) suspicious_cluster_count: usize,
    pub(crate) evicted_session_ids: HashSet<u64>,
    pub(crate) eviction_summaries: HashMap<u64, ClusterEvictionSummary>,
    pub(crate) max_cluster_score: f64,
}

impl ClusterEvaluation {
    fn empty() -> Self {
        Self {
            suspicious_cluster_count: 0,
            evicted_session_ids: HashSet::new(),
            eviction_summaries: HashMap::new(),
            max_cluster_score: 0.0,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClusterEvaluationJob {
    pub(crate) pressure: f64,
    pub(crate) now_ms: u64,
    pub(crate) live_sessions: Arc<[Arc<LiveSessionRecord>]>,
    pub(crate) recent_signatures: Arc<[HistoricalSignature]>,
}

#[derive(Clone)]
pub(crate) struct HistoricalSignature {
    source_session_id: u64,
    recorded_at_ms: u64,
    span_ms: u64,
    fingerprint: WindowFingerprint,
}

#[derive(Clone)]
struct DerivedInputEvent {
    at_ms: u64,
    bytes: SmallVec<[u8; MAX_SAMPLE_BYTES]>,
    mouse_motion: bool,
    raw_count: u32,
}

#[derive(Clone, Copy)]
struct ParsedMouseEvent {
    motion: bool,
    coord_bucket: u16,
}

struct DisjointSet {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl NetworkPrefix {
    pub(crate) fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(addr) => Self::V4(prefix_v4(addr)),
            IpAddr::V6(addr) => Self::V6(prefix_v6(addr)),
        }
    }
}

pub(crate) fn compute_pressure(running: usize, max_running: usize) -> f64 {
    if max_running == 0 || max_running == usize::MAX {
        return 0.0;
    }

    ((running as f64 / max_running as f64) / PRESSURE_SATURATION_OCCUPANCY).clamp(0.0, 1.0)
}

pub(crate) fn trim_old_inputs(inputs: &mut VecDeque<InputSample>, now_ms: u64) {
    while inputs
        .front()
        .is_some_and(|sample| now_ms.saturating_sub(sample.at_ms) > INPUT_WINDOW_MS)
    {
        inputs.pop_front();
    }
}

pub(crate) fn trim_old_outputs(outputs: &mut VecDeque<OutputSample>, now_ms: u64) {
    while outputs
        .front()
        .is_some_and(|sample| now_ms.saturating_sub(sample.at_ms) > INPUT_WINDOW_MS)
    {
        outputs.pop_front();
    }
}

pub(crate) fn trim_recent_signatures(
    recent_signatures: &mut VecDeque<HistoricalSignature>,
    now_ms: u64,
) {
    while recent_signatures.front().is_some_and(|signature| {
        now_ms.saturating_sub(signature.recorded_at_ms) > SIGNATURE_RETENTION_MS
    }) {
        recent_signatures.pop_front();
    }
    while recent_signatures.len() > MAX_RECENT_SIGNATURES {
        recent_signatures.pop_front();
    }
}

pub(crate) fn archive_signatures(
    record: &LiveSessionRecord,
    now_ms: u64,
    recent_signatures: &mut VecDeque<HistoricalSignature>,
) {
    for fingerprint in build_window_fingerprints(record, now_ms) {
        recent_signatures.push_back(HistoricalSignature {
            source_session_id: record.id,
            recorded_at_ms: now_ms,
            span_ms: fingerprint.span_ms,
            fingerprint,
        });
    }
    trim_recent_signatures(recent_signatures, now_ms);
}

pub(crate) fn evaluate_job(job: &ClusterEvaluationJob) -> ClusterEvaluation {
    evaluate_clusters(
        job.live_sessions.as_ref(),
        job.recent_signatures.as_ref(),
        job.pressure,
        job.now_ms,
    )
}

fn evaluate_clusters(
    live_sessions: &[Arc<LiveSessionRecord>],
    recent_signatures: &[HistoricalSignature],
    pressure: f64,
    now_ms: u64,
) -> ClusterEvaluation {
    let mut sessions: Vec<SessionAssessment> = live_sessions
        .iter()
        .filter_map(|session| SessionAssessment::from_record(session.as_ref(), now_ms))
        .collect();
    apply_replay_evidence(&mut sessions, recent_signatures, now_ms);
    if sessions.len() < MIN_CLUSTER_SIZE || pressure < LOW_PRESSURE_FLOOR {
        return ClusterEvaluation::empty();
    }

    let mut edges = Vec::new();
    let mut dsu = DisjointSet::new(sessions.len());
    for i in 0..sessions.len() {
        for j in (i + 1)..sessions.len() {
            if let Some(edge) = pair_evidence(&sessions[i], &sessions[j], pressure) {
                dsu.union(i, j);
                edges.push(edge);
            }
        }
    }
    materialize_replay_cohort(&sessions, pressure, &mut dsu, &mut edges);

    if edges.is_empty() {
        return ClusterEvaluation::empty();
    }

    let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
    for idx in 0..sessions.len() {
        let root = dsu.find(idx);
        components.entry(root).or_default().push(idx);
    }

    let mut edge_scores: HashMap<(u64, u64), f64> = HashMap::new();
    for edge in &edges {
        edge_scores
            .entry(edge_key(edge.a, edge.b))
            .and_modify(|score| *score = score.max(edge.score))
            .or_insert(edge.score);
    }

    let mut suspicious_cluster_count = 0usize;
    let mut evicted_session_ids = HashSet::new();
    let mut eviction_summaries = HashMap::new();
    let mut max_cluster_score: f64 = 0.0;
    let required_score = required_cluster_score(pressure);

    for component in components.into_values() {
        if component.len() < MIN_CLUSTER_SIZE {
            continue;
        }
        let factors = cluster_scores(&sessions, &component, &edge_scores, pressure);
        let score = factors.score();
        if score < required_score {
            continue;
        }
        max_cluster_score = max_cluster_score.max(score);
        suspicious_cluster_count += 1;

        let summary = cluster_eviction_summary(factors, component.len(), score, required_score);
        for &idx in &component {
            let session_id = sessions[idx].session_id;
            evicted_session_ids.insert(session_id);
            eviction_summaries.insert(session_id, summary);
        }
    }

    ClusterEvaluation {
        suspicious_cluster_count,
        evicted_session_ids,
        eviction_summaries,
        max_cluster_score,
    }
}

fn materialize_replay_cohort(
    sessions: &[SessionAssessment],
    pressure: f64,
    dsu: &mut DisjointSet,
    edges: &mut Vec<PairEvidence>,
) {
    if pressure < 0.60 {
        return;
    }
    let threshold = replay_session_threshold(pressure);
    let cohort = sessions
        .iter()
        .enumerate()
        .filter(|(_, session)| {
            session.replay_score >= threshold
                && session.replay_match_count > 0
                && session.coupling_deficit >= 0.18
        })
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    if cohort.len() < MIN_CLUSTER_SIZE {
        return;
    }
    let affinity = replay_cohort_affinity(sessions, &cohort);
    if affinity < (0.78 - 0.12 * pressure).clamp(0.60, 0.78) {
        return;
    }
    for i in 0..cohort.len() {
        for j in (i + 1)..cohort.len() {
            let left = cohort[i];
            let right = cohort[j];
            dsu.union(left, right);
            edges.push(PairEvidence {
                a: sessions[left].session_id,
                b: sessions[right].session_id,
                score: affinity,
            });
        }
    }
}

fn cluster_scores(
    sessions: &[SessionAssessment],
    component: &[usize],
    edge_scores: &HashMap<(u64, u64), f64>,
    pressure: f64,
) -> ClusterScores {
    let mut edge_sum = 0.0;
    let mut edge_count = 0usize;
    let mut low_engagement_sum = 0.0;
    let mut replay_sum = 0.0;
    let mut replay_density_sum = 0.0;
    let mut coupling_deficit_sum = 0.0;
    let mut prefix_counts: HashMap<NetworkPrefix, usize> = HashMap::new();
    let mut oldest = u64::MAX;
    let mut newest = 0u64;

    for idx in component {
        low_engagement_sum += sessions[*idx].low_engagement;
        replay_sum += sessions[*idx].replay_score;
        replay_density_sum += sessions[*idx].replay_density;
        coupling_deficit_sum += sessions[*idx].coupling_deficit;
        *prefix_counts.entry(sessions[*idx].ip_prefix).or_default() += 1;
        oldest = oldest.min(sessions[*idx].started_at_ms);
        newest = newest.max(sessions[*idx].started_at_ms);
    }
    for i in 0..component.len() {
        for j in (i + 1)..component.len() {
            let a = sessions[component[i]].session_id;
            let b = sessions[component[j]].session_id;
            if let Some(score) = edge_scores.get(&edge_key(a, b)) {
                edge_sum += *score;
                edge_count += 1;
            }
        }
    }
    let network_cohesion = prefix_counts
        .values()
        .max()
        .map(|count| *count as f64 / component.len() as f64)
        .unwrap_or(0.0);
    let start_spread_ms = newest.saturating_sub(oldest);
    let avg_edge = if edge_count == 0 {
        0.0
    } else {
        edge_sum / edge_count as f64
    };
    let avg_low_engagement = low_engagement_sum / component.len() as f64;
    ClusterScores {
        avg_edge,
        avg_replay: replay_sum / component.len() as f64,
        avg_replay_density: replay_density_sum / component.len() as f64,
        avg_low_engagement,
        avg_coupling_deficit: coupling_deficit_sum / component.len() as f64,
        network_cohesion,
        start_sync: (1.0 - start_spread_ms as f64 / (5.0 * 60_000.0)).clamp(0.0, 1.0),
        size_factor: ((component.len() as f64 - 1.0) / 6.0).clamp(0.0, 1.0),
        pressure,
        edge_low_engagement_interaction: avg_edge * avg_low_engagement,
    }
}

fn required_cluster_score(pressure: f64) -> f64 {
    (0.82 - (pressure - LOW_PRESSURE_FLOOR).max(0.0) * 0.42).clamp(0.56, 0.82)
}

impl ClusterScores {
    fn score(&self) -> f64 {
        if self.size_factor <= 0.34
            && self.avg_low_engagement < 0.48
            && self.avg_replay < 0.84
            && self.avg_edge < 0.73
        {
            return 0.0;
        }
        if self.avg_replay < 0.55 && self.avg_low_engagement < 0.70 && self.avg_edge < 0.73 {
            return 0.0;
        }
        if self.network_cohesion < 0.4 && self.avg_low_engagement < 0.44 && self.avg_replay < 0.88 {
            return 0.0;
        }
        let base = 0.48 * self.avg_edge
            + 0.20 * self.avg_replay
            + 0.09 * self.avg_replay_density
            + 0.09 * self.avg_low_engagement
            + 0.05 * self.avg_coupling_deficit
            + 0.04 * self.network_cohesion
            + 0.03 * self.start_sync;
        (base
            + 0.07 * self.size_factor
            + 0.04 * self.pressure
            + 0.08 * self.edge_low_engagement_interaction)
            .clamp(0.0, 1.0)
    }

    fn contributions(&self) -> Self {
        Self {
            avg_edge: 0.48 * self.avg_edge,
            avg_replay: 0.20 * self.avg_replay,
            avg_replay_density: 0.09 * self.avg_replay_density,
            avg_low_engagement: 0.09 * self.avg_low_engagement,
            avg_coupling_deficit: 0.05 * self.avg_coupling_deficit,
            network_cohesion: 0.04 * self.network_cohesion,
            start_sync: 0.03 * self.start_sync,
            size_factor: 0.07 * self.size_factor,
            pressure: 0.04 * self.pressure,
            edge_low_engagement_interaction: 0.08 * self.edge_low_engagement_interaction,
        }
    }
}

fn cluster_eviction_summary(
    factors: ClusterScores,
    cluster_size: usize,
    score: f64,
    required_score: f64,
) -> ClusterEvictionSummary {
    ClusterEvictionSummary {
        cluster_size,
        score,
        required_score,
        score_margin: score - required_score,
        factors,
        contributions: factors.contributions(),
    }
}

fn pair_evidence(
    left: &SessionAssessment,
    right: &SessionAssessment,
    pressure: f64,
) -> Option<PairEvidence> {
    let window_similarity = session_window_similarity(left, right);
    let session_confidence = min_session_confidence(left, right);
    if session_confidence < 0.24 {
        return None;
    }
    let low_engagement_affinity = similarity(left.low_engagement, right.low_engagement);
    let low_engagement_level = left.low_engagement.min(right.low_engagement);
    let replay_affinity = (left.replay_score.min(right.replay_score)
        * (0.5 + 0.5 * similarity(left.replay_density, right.replay_density)))
    .clamp(0.0, 1.0);
    let replay_match_affinity = similarity(
        (left.replay_match_count as f64 / 4.0).clamp(0.0, 1.0),
        (right.replay_match_count as f64 / 4.0).clamp(0.0, 1.0),
    );
    let coupling_affinity = similarity(left.coupling_deficit, right.coupling_deficit);
    let spectral_affinity = mean([
        similarity(left.spectral_peakiness, right.spectral_peakiness),
        similarity(left.spectral_entropy_norm, right.spectral_entropy_norm),
    ]);
    let start_similarity = 1.0
        - (left.started_at_ms.abs_diff(right.started_at_ms) as f64 / (5.0 * 60_000.0))
            .clamp(0.0, 1.0);
    let network_hint = if left.client_ip == right.client_ip {
        1.0
    } else if left.ip_prefix == right.ip_prefix {
        0.65
    } else {
        0.0
    };
    let hover_relief = left.mouse_motion_ratio.min(right.mouse_motion_ratio)
        * left.mouse_coord_entropy.min(right.mouse_coord_entropy)
        * left.spectral_entropy_norm.min(right.spectral_entropy_norm);
    let score = 0.62 * window_similarity
        + 0.10 * low_engagement_affinity
        + 0.08 * low_engagement_level
        + 0.10 * replay_affinity
        + 0.04 * replay_match_affinity
        + 0.03 * coupling_affinity
        + 0.01 * spectral_affinity
        + 0.01 * start_similarity
        + 0.01 * network_hint;
    let adjusted = (0.86 * score + 0.14 * session_confidence).clamp(0.0, 1.0);
    let dynamic_threshold = if pressure < 0.60 {
        0.78
    } else if pressure < 0.80 {
        0.72
    } else {
        0.67
    };
    let mut threshold: f64 = dynamic_threshold;
    if replay_affinity > 0.78 || network_hint > 0.0 {
        threshold -= 0.03;
    }
    if low_engagement_level > 0.68 && left.coupling_deficit.min(right.coupling_deficit) > 0.80 {
        threshold -= 0.04;
    }
    if hover_relief > 0.18 && replay_affinity < 0.45 {
        threshold += 0.06;
    }
    if left.spectral_peakiness.min(right.spectral_peakiness) > 0.62
        && left.mouse_motion_ratio.min(right.mouse_motion_ratio) > 0.35
    {
        threshold -= 0.03;
    }
    threshold = threshold.clamp(0.56, 0.84);
    if adjusted < threshold {
        return None;
    }
    Some(PairEvidence {
        a: left.session_id,
        b: right.session_id,
        score: adjusted.clamp(0.0, 1.0),
    })
}

impl SessionAssessment {
    fn from_record(record: &LiveSessionRecord, now_ms: u64) -> Option<Self> {
        let windows = build_window_fingerprints(record, now_ms);
        if windows.is_empty() {
            return None;
        }
        let avg = |f: fn(&WindowFingerprint) -> f64| weighted_window_average(&windows, f);
        let avg_event_rate = avg(|window| window.event_rate_norm);
        let avg_entropy = avg(|window| window.entropy_norm);
        let avg_innovation = avg(|window| window.ngram_innovation);
        let avg_coupling = avg(|window| window.output_coupling);
        let avg_balance = avg(|window| window.input_output_balance);
        let avg_mouse_motion = avg(|window| window.mouse_motion_ratio);
        let avg_mouse_coord_entropy = avg(|window| window.mouse_coord_entropy);
        let avg_spectral_peak = avg(|window| window.spectral_peakiness);
        let avg_spectral_entropy = avg(|window| window.spectral_entropy_norm);
        let coupling_deficit = (1.0 - avg_coupling).clamp(0.0, 1.0);
        let mouse_liveness = (avg_mouse_motion
            * (0.35 + 0.65 * avg_mouse_coord_entropy)
            * (0.55 + 0.45 * avg_spectral_entropy))
            .clamp(0.0, 1.0);
        let mechanicality = (0.60 * avg_spectral_peak
            + 0.40 * avg_mouse_motion * (1.0 - avg_mouse_coord_entropy))
            .clamp(0.0, 1.0);
        let interactivity = 0.24 * avg_event_rate
            + 0.18 * avg_entropy
            + 0.14 * avg_innovation
            + 0.14 * avg_coupling
            + 0.10 * avg_balance
            + 0.10 * avg_spectral_entropy
            + 0.10 * mouse_liveness;
        let low_engagement = (1.0 - interactivity + 0.30 * mechanicality).clamp(0.0, 1.0);
        Some(Self {
            session_id: record.id,
            client_ip: record.client_ip,
            ip_prefix: record.ip_prefix,
            started_at_ms: record.started_at_ms,
            low_engagement,
            replay_score: 0.0,
            replay_match_count: 0,
            replay_density: 0.0,
            coupling_deficit,
            mouse_motion_ratio: avg_mouse_motion,
            mouse_coord_entropy: avg_mouse_coord_entropy,
            spectral_peakiness: avg_spectral_peak,
            spectral_entropy_norm: avg_spectral_entropy,
            windows,
        })
    }
}

fn apply_replay_evidence(
    sessions: &mut [SessionAssessment],
    recent_signatures: &[HistoricalSignature],
    now_ms: u64,
) {
    if recent_signatures.is_empty() {
        return;
    }
    for session in sessions {
        let mut best_by_span: HashMap<u64, f64> = HashMap::new();
        let mut distinct_sources: HashMap<u64, f64> = HashMap::new();
        for window in &session.windows {
            for signature in recent_signatures.iter().filter(|signature| {
                signature.span_ms == window.span_ms
                    && signature.recorded_at_ms <= now_ms
                    && now_ms.saturating_sub(signature.recorded_at_ms) <= SIGNATURE_RETENTION_MS
            }) {
                let score = window_similarity(window, &signature.fingerprint);
                best_by_span
                    .entry(window.span_ms)
                    .and_modify(|current| *current = current.max(score))
                    .or_insert(score);
                if score >= 0.78 {
                    distinct_sources
                        .entry(signature.source_session_id)
                        .and_modify(|current| *current = current.max(score))
                        .or_insert(score);
                }
            }
        }
        let weighted_best = session
            .windows
            .iter()
            .map(|window| {
                window_weight(window.span_ms)
                    * best_by_span
                        .get(&window.span_ms)
                        .map_or(0.0, |score| *score)
            })
            .sum::<f64>();
        let mut strongest = distinct_sources.values().collect::<Vec<_>>();
        strongest
            .sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
        let replay_density = if strongest.is_empty() {
            0.0
        } else {
            strongest.iter().take(3).map(|score| **score).sum::<f64>()
                / strongest.len().min(3) as f64
        };
        session.replay_match_count = distinct_sources.len();
        session.replay_density = replay_density;
        session.replay_score = (0.82 * weighted_best
            + 0.12 * replay_density
            + 0.06 * (session.replay_match_count as f64 / 3.0).clamp(0.0, 1.0))
        .clamp(0.0, 1.0);
    }
}

fn replay_session_threshold(pressure: f64) -> f64 {
    if pressure < 0.70 {
        0.86
    } else if pressure < 0.85 {
        0.80
    } else {
        0.72
    }
}

fn replay_cohort_affinity(sessions: &[SessionAssessment], indices: &[usize]) -> f64 {
    let avg_replay = mean(indices.iter().map(|idx| sessions[*idx].replay_score));
    let avg_density = mean(indices.iter().map(|idx| sessions[*idx].replay_density));
    let avg_coupling_deficit = mean(indices.iter().map(|idx| sessions[*idx].coupling_deficit));
    (0.62 * avg_replay + 0.22 * avg_density + 0.16 * avg_coupling_deficit).clamp(0.0, 1.0)
}

fn build_window_fingerprints(record: &LiveSessionRecord, now_ms: u64) -> Vec<WindowFingerprint> {
    [SHORT_WINDOW_MS, MEDIUM_WINDOW_MS, INPUT_WINDOW_MS]
        .iter()
        .filter_map(|span_ms| build_window_fingerprint(record, now_ms, *span_ms))
        .collect()
}

fn build_window_fingerprint(
    record: &LiveSessionRecord,
    now_ms: u64,
    span_ms: u64,
) -> Option<WindowFingerprint> {
    let window_start = now_ms.saturating_sub(span_ms);
    let inputs = record
        .inputs
        .iter()
        .filter(|sample| sample.at_ms >= window_start)
        .collect::<Vec<_>>();
    if inputs.len() < MIN_INPUTS_FOR_FINGERPRINT {
        return None;
    }
    let outputs = record
        .outputs
        .iter()
        .filter(|sample| sample.at_ms >= window_start)
        .collect::<Vec<_>>();
    let window_anchor = record.started_at_ms.max(window_start);
    let window_ms = now_ms.saturating_sub(window_anchor).max(1);
    let total_output_bytes = outputs
        .iter()
        .map(|sample| sample.bytes as usize)
        .sum::<usize>();
    let mut derived_inputs: Vec<DerivedInputEvent> = Vec::with_capacity(inputs.len());
    let mut byte_counts = [0u32; 256];
    let mut class_counts = [0u32; CLASS_BUCKETS];
    let mut gap_hist = [0.0; GAP_BUCKETS];
    let mut response_hist = [0.0; RESPONSE_BUCKETS];
    let mut bucket_counts = vec![0u32; (window_ms / BURST_BUCKET_MS).max(1) as usize + 1];
    let mut token_counts: HashMap<u64, u32> = HashMap::new();
    let mut transition_counts: HashMap<(u64, u64), u32> = HashMap::new();
    let mut source_counts: HashMap<u64, u32> = HashMap::new();
    let mut unigram_weights = [0i32; 64];
    let mut bigram_weights = [0i32; 64];
    let mut trigram_weights = [0i32; 64];
    let mut transition_weights = [0i32; 64];
    let mut tokens = Vec::with_capacity(inputs.len());
    let mut distinct_bigrams = HashSet::new();
    let mut distinct_trigrams = HashSet::new();
    let mut total_input_bytes = 0usize;
    let mut output_cursor = 0usize;
    let mut last_output_at = None;
    let mut gaps = Vec::with_capacity(inputs.len().saturating_sub(1));
    let mut mouse_events = 0usize;
    let mut mouse_motion_events = 0usize;
    let mut mouse_coord_counts: HashMap<u16, u32> = HashMap::new();

    for sample in &inputs {
        let parsed_mouse = parse_terminal_mouse_event(&sample.bytes);
        if let Some(mouse) = parsed_mouse {
            mouse_events += 1;
            if mouse.motion {
                mouse_motion_events += 1;
                *mouse_coord_counts.entry(mouse.coord_bucket).or_default() += 1;
            }
        }
        if let Some(_mouse) = parsed_mouse.filter(|mouse| mouse.motion) {
            if let Some(last) = derived_inputs.last_mut() {
                if last.mouse_motion
                    && sample.at_ms.saturating_sub(last.at_ms) <= MOUSE_MOTION_COALESCE_MS
                {
                    last.at_ms = sample.at_ms;
                    last.bytes = sample.bytes.clone();
                    last.raw_count = last.raw_count.saturating_add(1);
                    continue;
                }
            }
        }
        derived_inputs.push(DerivedInputEvent {
            at_ms: sample.at_ms,
            bytes: sample.bytes.clone(),
            mouse_motion: parsed_mouse.is_some_and(|mouse| mouse.motion),
            raw_count: 1,
        });
    }

    if derived_inputs.len() < MIN_INPUTS_FOR_FINGERPRINT && mouse_motion_events < 12 {
        return None;
    }

    for (index, event) in derived_inputs.iter().enumerate() {
        while output_cursor < outputs.len() && outputs[output_cursor].at_ms <= event.at_ms {
            last_output_at = Some(outputs[output_cursor].at_ms);
            output_cursor += 1;
        }
        let gap = if index == 0 {
            event.at_ms.saturating_sub(window_anchor)
        } else {
            event.at_ms.saturating_sub(derived_inputs[index - 1].at_ms)
        };
        if index > 0 {
            gaps.push(gap);
        }
        let gap_bucket = quantize_gap(gap) as usize;
        let response_bucket =
            quantize_response_lag(last_output_at.map(|at| event.at_ms.saturating_sub(at)));
        gap_hist[gap_bucket] += 1.0;
        response_hist[response_bucket] += 1.0;
        let len_bucket = quantize_len(event.bytes.len());
        let mask = class_mask(&event.bytes);
        let phase_bucket = (((event.at_ms % IDLE_TIMEOUT_MS) * 8) / IDLE_TIMEOUT_MS).min(7);
        let mouse_token = parse_terminal_mouse_event(&event.bytes)
            .map(|mouse| {
                ((u64::from(mouse.coord_bucket) & 0xff) << 8)
                    | if mouse.motion { 1u64 } else { 2u64 }
            })
            .unwrap_or(0);
        let token = splitmix64(
            ((gap_bucket as u64) << 24)
                | ((response_bucket as u64) << 20)
                | ((len_bucket & 0x0f) << 12)
                | ((mask as u64) << 4)
                | phase_bucket
                | (mouse_token << 28),
        );
        tokens.push(token);
        *token_counts.entry(token).or_default() += 1;
        accumulate_hash(&mut unigram_weights, token);
        if index > 0 {
            let pair_hash = splitmix64(token ^ tokens[index - 1].rotate_left(7));
            distinct_bigrams.insert(pair_hash);
            accumulate_hash(&mut bigram_weights, pair_hash);
            *transition_counts
                .entry((tokens[index - 1], token))
                .or_default() += 1;
            *source_counts.entry(tokens[index - 1]).or_default() += 1;
            accumulate_hash(&mut transition_weights, pair_hash.rotate_left(11));
        }
        if index > 1 {
            let trigram_hash = splitmix64(
                token ^ tokens[index - 1].rotate_left(11) ^ tokens[index - 2].rotate_left(23),
            );
            distinct_trigrams.insert(trigram_hash);
            accumulate_hash(&mut trigram_weights, trigram_hash);
        }
        let bucket = ((event.at_ms.saturating_sub(window_anchor)) / BURST_BUCKET_MS) as usize;
        if let Some(count) = bucket_counts.get_mut(bucket) {
            *count = count.saturating_add(event.raw_count.min(4));
        }
        total_input_bytes += if event.mouse_motion {
            3 * event.raw_count.min(4) as usize
        } else {
            event.bytes.len()
        };
        for byte in &event.bytes {
            byte_counts[*byte as usize] += 1;
            for (class_idx, present) in class_presence(*byte).iter().enumerate() {
                if *present {
                    class_counts[class_idx] += 1;
                }
            }
        }
    }

    normalize_bins(&mut gap_hist);
    normalize_bins(&mut response_hist);
    let class_total = class_counts.iter().sum::<u32>().max(1) as f64;
    let mut class_hist = [0.0; CLASS_BUCKETS];
    for (idx, count) in class_counts.iter().enumerate() {
        class_hist[idx] = *count as f64 / class_total;
    }
    let (spectral_peakiness, spectral_entropy_norm) = spectral_features(&bucket_counts);
    let mouse_coord_entropy =
        normalized_bucket_entropy(mouse_coord_counts.values().map(|count| *count));
    let mouse_ratio = (mouse_events as f64 / inputs.len() as f64).clamp(0.0, 1.0);
    let mouse_motion_ratio = (mouse_motion_events as f64 / inputs.len() as f64).clamp(0.0, 1.0);
    let mouse_flood_score = if derived_inputs.is_empty() {
        0.0
    } else {
        ((mouse_motion_events as f64 / derived_inputs.len() as f64) / 6.0).clamp(0.0, 1.0)
    };

    let confidence = ((derived_inputs.len() as f64 / 12.0).min(1.0)
        * (0.35 + 0.65 * (window_ms as f64 / span_ms as f64).clamp(0.0, 1.0)))
    .clamp(0.0, 1.0);

    Some(WindowFingerprint {
        span_ms,
        confidence,
        event_rate_norm: (derived_inputs.len() as f64 * 60_000.0 / window_ms as f64 / 24.0)
            .clamp(0.0, 1.0),
        entropy_norm: normalized_entropy(&byte_counts, total_input_bytes.max(1) as f64),
        transition_entropy_norm: normalized_transition_entropy(&transition_counts, &source_counts),
        ngram_innovation: mean([
            (token_counts.len() as f64 / derived_inputs.len().max(1) as f64).clamp(0.0, 1.0),
            (distinct_bigrams.len() as f64 / derived_inputs.len().saturating_sub(1).max(1) as f64)
                .clamp(0.0, 1.0),
            (distinct_trigrams.len() as f64 / derived_inputs.len().saturating_sub(2).max(1) as f64)
                .clamp(0.0, 1.0),
        ]),
        burstiness: fano_norm(&bucket_counts),
        periodicity_60s: periodicity_60s(&derived_inputs),
        spectral_peakiness,
        spectral_entropy_norm,
        input_output_balance: ((total_input_bytes as f64 * 24.0)
            / total_output_bytes.max(1) as f64)
            .clamp(0.0, 1.0),
        output_coupling: output_coupling_score(&response_hist),
        autocorr1: autocorrelation_norm(&gaps, 1),
        autocorr2: autocorrelation_norm(&gaps, 2),
        mouse_ratio,
        mouse_motion_ratio,
        mouse_coord_entropy,
        mouse_flood_score,
        gap_hist,
        response_hist,
        class_hist,
        unigram_hash: collapse_simhash(&unigram_weights),
        bigram_hash: collapse_simhash(&bigram_weights),
        trigram_hash: collapse_simhash(&trigram_weights),
        transition_hash: collapse_simhash(&transition_weights),
    })
}

fn prefix_v4(addr: Ipv4Addr) -> u32 {
    u32::from(addr) & 0xFFFF_FF00
}

fn prefix_v6(addr: Ipv6Addr) -> u64 {
    let octets = addr.octets();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&octets[..8]);
    u64::from_be_bytes(bytes)
}

fn min_session_confidence(left: &SessionAssessment, right: &SessionAssessment) -> f64 {
    matching_window_average(
        &left.windows,
        &right.windows,
        |left, _| window_weight(left.span_ms),
        |left, right| (left.confidence * right.confidence).sqrt(),
    )
}

fn weighted_window_average(
    windows: &[WindowFingerprint],
    f: impl Fn(&WindowFingerprint) -> f64,
) -> f64 {
    weighted_average(windows.iter().map(|window| {
        (
            window_weight(window.span_ms) * window.confidence.max(0.1),
            f(window),
        )
    }))
}

fn session_window_similarity(left: &SessionAssessment, right: &SessionAssessment) -> f64 {
    matching_window_average(
        &left.windows,
        &right.windows,
        |left, right| window_weight(left.span_ms) * left.confidence * right.confidence,
        window_similarity,
    )
}

fn window_similarity(left: &WindowFingerprint, right: &WindowFingerprint) -> f64 {
    let hash_similarity = mean([
        hamming_similarity(left.unigram_hash, right.unigram_hash),
        hamming_similarity(left.bigram_hash, right.bigram_hash),
        hamming_similarity(left.trigram_hash, right.trigram_hash),
        hamming_similarity(left.transition_hash, right.transition_hash),
    ]);
    let histogram_similarity = mean([
        cosine_similarity(&left.gap_hist, &right.gap_hist),
        cosine_similarity(&left.response_hist, &right.response_hist),
        cosine_similarity(&left.class_hist, &right.class_hist),
    ]);
    let scalar_similarity = mean([
        similarity(left.event_rate_norm, right.event_rate_norm),
        similarity(left.entropy_norm, right.entropy_norm),
        similarity(left.transition_entropy_norm, right.transition_entropy_norm),
        similarity(left.ngram_innovation, right.ngram_innovation),
        similarity(left.burstiness, right.burstiness),
        similarity(left.periodicity_60s, right.periodicity_60s),
        similarity(left.spectral_peakiness, right.spectral_peakiness),
        similarity(left.spectral_entropy_norm, right.spectral_entropy_norm),
        similarity(left.input_output_balance, right.input_output_balance),
        similarity(left.output_coupling, right.output_coupling),
        similarity(left.autocorr1, right.autocorr1),
        similarity(left.autocorr2, right.autocorr2),
        similarity(left.mouse_ratio, right.mouse_ratio),
        similarity(left.mouse_motion_ratio, right.mouse_motion_ratio),
        similarity(left.mouse_coord_entropy, right.mouse_coord_entropy),
        similarity(left.mouse_flood_score, right.mouse_flood_score),
    ]);
    (0.42 * hash_similarity
        + 0.28 * histogram_similarity
        + 0.22 * scalar_similarity
        + 0.08 * left.confidence.min(right.confidence))
    .clamp(0.0, 1.0)
}

fn mean(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in values {
        sum += value;
        count += 1;
    }
    if count == 0 { 0.0 } else { sum / count as f64 }
}

fn weighted_average(values: impl IntoIterator<Item = (f64, f64)>) -> f64 {
    let mut weighted_sum = 0.0;
    let mut total_weight = 0.0;
    for (weight, value) in values {
        weighted_sum += weight * value;
        total_weight += weight;
    }
    if total_weight == 0.0 {
        0.0
    } else {
        weighted_sum / total_weight
    }
}

fn matching_window_average(
    left: &[WindowFingerprint],
    right: &[WindowFingerprint],
    weight: impl Fn(&WindowFingerprint, &WindowFingerprint) -> f64,
    value: impl Fn(&WindowFingerprint, &WindowFingerprint) -> f64,
) -> f64 {
    weighted_average(left.iter().filter_map(|left_window| {
        right
            .iter()
            .find(|right_window| right_window.span_ms == left_window.span_ms)
            .map(|right_window| {
                (
                    weight(left_window, right_window),
                    value(left_window, right_window),
                )
            })
    }))
}

fn similarity(left: f64, right: f64) -> f64 {
    1.0 - (left - right).abs().clamp(0.0, 1.0)
}

fn cosine_similarity<const N: usize>(left: &[f64; N], right: &[f64; N]) -> f64 {
    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;
    for idx in 0..N {
        dot += left[idx] * right[idx];
        left_norm += left[idx] * left[idx];
        right_norm += right[idx] * right[idx];
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        (dot / (left_norm.sqrt() * right_norm.sqrt())).clamp(0.0, 1.0)
    }
}

fn hamming_similarity(left: u64, right: u64) -> f64 {
    1.0 - (left ^ right).count_ones() as f64 / 64.0
}

fn window_weight(span_ms: u64) -> f64 {
    match span_ms {
        SHORT_WINDOW_MS => 0.26,
        MEDIUM_WINDOW_MS => 0.34,
        _ => 0.40,
    }
}

fn normalized_entropy(counts: &[u32; 256], total: f64) -> f64 {
    if total <= 0.0 {
        return 0.0;
    }
    let mut entropy = 0.0;
    for count in counts {
        if *count == 0 {
            continue;
        }
        let p = *count as f64 / total;
        entropy -= p * p.log2();
    }
    (entropy / 8.0).clamp(0.0, 1.0)
}

fn normalized_transition_entropy(
    transition_counts: &HashMap<(u64, u64), u32>,
    source_counts: &HashMap<u64, u32>,
) -> f64 {
    if transition_counts.is_empty() || source_counts.is_empty() {
        return 0.0;
    }
    let total = source_counts.values().sum::<u32>().max(1) as f64;
    let mut entropy = 0.0;
    for (source, source_count) in source_counts {
        let mut conditional = 0.0;
        for ((edge_source, _), count) in transition_counts {
            if edge_source != source {
                continue;
            }
            let p = *count as f64 / *source_count as f64;
            conditional -= p * p.log2();
        }
        entropy += (*source_count as f64 / total) * conditional;
    }
    (entropy / 6.0).clamp(0.0, 1.0)
}

fn normalize_bins<const N: usize>(bins: &mut [f64; N]) {
    let total = bins.iter().sum::<f64>();
    if total <= 0.0 {
        return;
    }
    for bin in bins.iter_mut() {
        *bin /= total;
    }
}

fn normalized_bucket_entropy(values: impl Iterator<Item = u32>) -> f64 {
    let mut num_categories = 0usize;
    let mut total = 0u64;
    let mut weighted_log_sum = 0.0;
    for count in values {
        if count == 0 {
            continue;
        }
        num_categories += 1;
        total += count as u64;
        weighted_log_sum += count as f64 * (count as f64).log2();
    }
    let total = total as f64;
    if total <= 0.0 {
        return 0.0;
    }
    let entropy = total.log2() - weighted_log_sum / total;
    let max_entropy = (num_categories.max(2) as f64).log2();
    if max_entropy <= 0.0 {
        0.0
    } else {
        (entropy / max_entropy).clamp(0.0, 1.0)
    }
}

fn spectral_features(counts: &[u32]) -> (f64, f64) {
    if counts.len() < 4 {
        return (0.0, 0.0);
    }
    let n = counts.len();
    let mean = counts.iter().sum::<u32>() as f64 / n as f64;
    let mut magnitudes = Vec::with_capacity(n / 2);
    for harmonic in 1..=(n / 2) {
        let mut real = 0.0;
        let mut imag = 0.0;
        for (idx, count) in counts.iter().enumerate() {
            let centered = *count as f64 - mean;
            let angle = std::f64::consts::TAU * harmonic as f64 * idx as f64 / n as f64;
            real += centered * angle.cos();
            imag -= centered * angle.sin();
        }
        magnitudes.push((real * real + imag * imag).sqrt());
    }
    let total = magnitudes.iter().sum::<f64>();
    if total <= 0.0 {
        return (0.0, 0.0);
    }
    let peak = magnitudes
        .iter()
        .fold(0.0_f64, |peak, magnitude| peak.max(*magnitude))
        / total;
    let mut entropy = 0.0;
    for magnitude in &magnitudes {
        if *magnitude <= 0.0 {
            continue;
        }
        let p = *magnitude / total;
        entropy -= p * p.log2();
    }
    let max_entropy = (magnitudes.len().max(2) as f64).log2();
    let entropy_norm = if max_entropy <= 0.0 {
        0.0
    } else {
        (entropy / max_entropy).clamp(0.0, 1.0)
    };
    (peak.clamp(0.0, 1.0), entropy_norm)
}

fn parse_terminal_mouse_event(bytes: &[u8]) -> Option<ParsedMouseEvent> {
    parse_sgr_mouse_event(bytes).or_else(|| parse_legacy_mouse_event(bytes))
}

fn parse_sgr_mouse_event(bytes: &[u8]) -> Option<ParsedMouseEvent> {
    if bytes.len() < 6 || !bytes.starts_with(b"\x1b[<") {
        return None;
    }
    let mut idx = 3usize;
    let cb = parse_mouse_number(bytes, &mut idx)?;
    if *bytes.get(idx)? != b';' {
        return None;
    }
    idx += 1;
    let x = parse_mouse_number(bytes, &mut idx)?;
    if *bytes.get(idx)? != b';' {
        return None;
    }
    idx += 1;
    let y = parse_mouse_number(bytes, &mut idx)?;
    let terminator = *bytes.get(idx)?;
    if terminator != b'M' && terminator != b'm' {
        return None;
    }
    Some(ParsedMouseEvent {
        motion: (cb & 32) != 0,
        coord_bucket: quantize_mouse_coord(x, y),
    })
}

fn parse_legacy_mouse_event(bytes: &[u8]) -> Option<ParsedMouseEvent> {
    if bytes.len() < 6 || !bytes.starts_with(b"\x1b[M") {
        return None;
    }
    let cb = bytes[3].saturating_sub(32);
    let x = bytes[4].saturating_sub(32) as u16;
    let y = bytes[5].saturating_sub(32) as u16;
    Some(ParsedMouseEvent {
        motion: (cb & 32) != 0,
        coord_bucket: quantize_mouse_coord(x, y),
    })
}

fn parse_mouse_number(bytes: &[u8], idx: &mut usize) -> Option<u16> {
    let mut value = 0u16;
    let mut seen = false;
    while let Some(byte) = bytes.get(*idx) {
        if !byte.is_ascii_digit() {
            break;
        }
        seen = true;
        value = value
            .saturating_mul(10)
            .saturating_add(u16::from(byte.saturating_sub(b'0')));
        *idx += 1;
    }
    seen.then_some(value)
}

fn quantize_mouse_coord(x: u16, y: u16) -> u16 {
    let x_bucket = (x.min(255) / 16) & 0x0f;
    let y_bucket = (y.min(255) / 16) & 0x0f;
    (x_bucket << 4) | y_bucket
}

fn quantize_gap(gap_ms: u64) -> u64 {
    match gap_ms {
        0..=40 => 0,
        41..=120 => 1,
        121..=300 => 2,
        301..=700 => 3,
        701..=1_500 => 4,
        1_501..=3_500 => 5,
        3_501..=8_000 => 6,
        8_001..=20_000 => 7,
        20_001..=40_000 => 8,
        40_001..=75_000 => 9,
        _ => 10,
    }
}

fn quantize_response_lag(response_lag: Option<u64>) -> usize {
    match response_lag.unwrap_or(u64::MAX) {
        0..=60 => 0,
        61..=200 => 1,
        201..=500 => 2,
        501..=1_000 => 3,
        1_001..=2_000 => 4,
        2_001..=5_000 => 5,
        5_001..=15_000 => 6,
        15_001..=60_000 => 7,
        _ => 8,
    }
}

fn class_presence(byte: u8) -> [bool; CLASS_BUCKETS] {
    [
        byte == 0x1b,
        byte.is_ascii_control(),
        byte.is_ascii_whitespace(),
        byte.is_ascii_alphabetic(),
        byte.is_ascii_digit(),
        byte.is_ascii_punctuation(),
    ]
}

fn fano_norm(counts: &[u32]) -> f64 {
    if counts.len() < 2 {
        return 0.0;
    }
    let mean = counts.iter().sum::<u32>() as f64 / counts.len() as f64;
    if mean <= 0.0 {
        return 0.0;
    }
    let variance = counts
        .iter()
        .map(|count| {
            let delta = *count as f64 - mean;
            delta * delta
        })
        .sum::<f64>()
        / counts.len() as f64;
    (variance / mean / 8.0).clamp(0.0, 1.0)
}

fn periodicity_60s(inputs: &[DerivedInputEvent]) -> f64 {
    if inputs.is_empty() {
        return 0.0;
    }
    let mut sum_cos = 0.0;
    let mut sum_sin = 0.0;
    for sample in inputs {
        let phase = (sample.at_ms % IDLE_TIMEOUT_MS) as f64 / IDLE_TIMEOUT_MS as f64;
        let angle = phase * std::f64::consts::TAU;
        sum_cos += angle.cos();
        sum_sin += angle.sin();
    }
    ((sum_cos.powi(2) + sum_sin.powi(2)).sqrt() / inputs.len() as f64).clamp(0.0, 1.0)
}

fn output_coupling_score(response_hist: &[f64; RESPONSE_BUCKETS]) -> f64 {
    let near = response_hist[0] + response_hist[1] + response_hist[2] + response_hist[3];
    let mid = response_hist[4] + response_hist[5];
    let far = response_hist[6] + response_hist[7] + response_hist[8];
    (0.75 * near + 0.25 * mid - 0.35 * far).clamp(0.0, 1.0)
}

fn autocorrelation_norm(values: &[u64], lag: usize) -> f64 {
    if values.len() <= lag + 1 {
        return 0.0;
    }
    let mean = values.iter().sum::<u64>() as f64 / values.len() as f64;
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for value in values {
        let centered = *value as f64 - mean;
        denominator += centered * centered;
    }
    if denominator <= 0.0 {
        return 0.0;
    }
    for idx in 0..(values.len() - lag) {
        numerator += (values[idx] as f64 - mean) * (values[idx + lag] as f64 - mean);
    }
    ((numerator / denominator) + 1.0)
        .mul_add(0.5, 0.0)
        .clamp(0.0, 1.0)
}

fn quantize_len(len: usize) -> u64 {
    match len {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=4 => 3,
        5..=8 => 4,
        9..=16 => 5,
        _ => 6,
    }
}

fn class_mask(bytes: &[u8]) -> u8 {
    let mut mask = 0u8;
    for byte in bytes {
        if *byte == 0x1b {
            mask |= 1 << 0;
        }
        if byte.is_ascii_control() {
            mask |= 1 << 1;
        }
        if byte.is_ascii_whitespace() {
            mask |= 1 << 2;
        }
        if byte.is_ascii_alphabetic() {
            mask |= 1 << 3;
        }
        if byte.is_ascii_digit() {
            mask |= 1 << 4;
        }
        if byte.is_ascii_punctuation() {
            mask |= 1 << 5;
        }
    }
    mask
}

fn accumulate_hash(weights: &mut [i32; 64], hash: u64) {
    for (idx, weight) in weights.iter_mut().enumerate() {
        if (hash >> idx) & 1 == 1 {
            *weight += 1;
        } else {
            *weight -= 1;
        }
    }
}

fn collapse_simhash(weights: &[i32; 64]) -> u64 {
    let mut hash = 0u64;
    for (idx, weight) in weights.iter().enumerate() {
        if *weight >= 0 {
            hash |= 1u64 << idx;
        }
    }
    hash
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn edge_key(a: u64, b: u64) -> (u64, u64) {
    if a < b { (a, b) } else { (b, a) }
}

impl DisjointSet {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            let root = self.find(self.parent[x]);
            self.parent[x] = root;
        }
        self.parent[x]
    }

    fn union(&mut self, a: usize, b: usize) {
        let mut root_a = self.find(a);
        let mut root_b = self.find(b);
        if root_a == root_b {
            return;
        }
        if self.rank[root_a] < self.rank[root_b] {
            std::mem::swap(&mut root_a, &mut root_b);
        }
        self.parent[root_b] = root_a;
        if self.rank[root_a] == self.rank[root_b] {
            self.rank[root_a] += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Transport;

    type Trace = Vec<(u64, Vec<u8>)>;

    const REPLAY_OUTPUTS: &[u64] = &[10_000, 38_000, 75_000, 112_000, 148_000, 181_000, 213_000];
    const BOT_OUTPUTS: &[u64] = &[
        10_000, 39_000, 71_000, 104_000, 138_000, 173_000, 209_000, 243_000,
    ];
    const HUMAN_OUTPUTS: &[u64] = &[
        9_000, 28_000, 49_000, 73_000, 98_000, 126_000, 154_000, 183_000,
    ];

    fn ipv4(value: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, value))
    }

    fn ipv4_in(subnet: u8, host: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(198, 51, subnet, host))
    }

    fn transport(idx: usize) -> Transport {
        if idx % 2 == 0 {
            Transport::Ssh
        } else {
            Transport::Web
        }
    }

    fn session(
        id: u64,
        ip: IpAddr,
        transport: Transport,
        started_at_ms: u64,
        input_events: Trace,
        output_offsets: &[u64],
    ) -> LiveSessionRecord {
        let mut inputs = VecDeque::with_capacity(input_events.len());
        for (at_ms, bytes) in input_events {
            let mut sample_bytes = SmallVec::<[u8; MAX_SAMPLE_BYTES]>::new();
            for byte in bytes.iter().take(MAX_SAMPLE_BYTES) {
                sample_bytes.push(*byte);
            }
            inputs.push_back(InputSample {
                at_ms,
                bytes: sample_bytes,
            });
        }
        LiveSessionRecord {
            id,
            client_ip: ip,
            ip_prefix: NetworkPrefix::from_ip(ip),
            transport,
            started_at_ms,
            inputs,
            outputs: output_offsets
                .iter()
                .map(|offset| OutputSample {
                    at_ms: started_at_ms + *offset,
                    bytes: 6_000,
                })
                .collect(),
        }
    }

    fn swarm(
        count: usize,
        base_id: u64,
        output_offsets: &[u64],
        mut build: impl FnMut(usize) -> (IpAddr, Transport, u64, Trace),
    ) -> Vec<LiveSessionRecord> {
        (0..count)
            .map(|idx| {
                let (ip, transport, started_at_ms, trace) = build(idx);
                session(
                    base_id + idx as u64,
                    ip,
                    transport,
                    started_at_ms,
                    trace,
                    output_offsets,
                )
            })
            .collect()
    }

    fn evaluate_case(
        records: Vec<LiveSessionRecord>,
        recent_signatures: VecDeque<HistoricalSignature>,
        pressure: f64,
        now_ms: u64,
    ) -> (ClusterEvaluation, String) {
        let debug = debug_summary(&records, &recent_signatures, pressure, now_ms);
        let mut live_sessions = Vec::with_capacity(records.len());
        for record in records {
            live_sessions.push(Arc::new(record));
        }
        let mut recent_signatures_vec = Vec::with_capacity(recent_signatures.len());
        for signature in recent_signatures {
            recent_signatures_vec.push(signature);
        }
        let evaluation =
            evaluate_clusters(&live_sessions, &recent_signatures_vec, pressure, now_ms);
        (evaluation, debug)
    }

    fn debug_summary(
        records: &[LiveSessionRecord],
        recent_signatures: &VecDeque<HistoricalSignature>,
        pressure: f64,
        now_ms: u64,
    ) -> String {
        let mut sessions = records
            .iter()
            .filter_map(|record| SessionAssessment::from_record(record, now_ms))
            .collect::<Vec<_>>();
        let recent_signatures = recent_signatures.iter().cloned().collect::<Vec<_>>();
        apply_replay_evidence(&mut sessions, &recent_signatures, now_ms);
        let mut lines = sessions
            .iter()
            .map(|session| {
                format!(
                    "session={}  low={:.3} replay={:.3} matches={} density={:.3} coupling={:.3} mouse={:.3}/{:.3} spectral={:.3}/{:.3}",
                    session.session_id,
                    session.low_engagement,
                    session.replay_score,
                    session.replay_match_count,
                    session.replay_density,
                    session.coupling_deficit,
                    session.mouse_motion_ratio,
                    session.mouse_coord_entropy,
                    session.spectral_peakiness,
                    session.spectral_entropy_norm,
                )
            })
            .collect::<Vec<_>>();
        for i in 0..sessions.len() {
            for j in (i + 1)..sessions.len() {
                lines.push(format!(
                    "pair={}-{} sim={:.3} conf={:.3} edge={:.3}",
                    sessions[i].session_id,
                    sessions[j].session_id,
                    session_window_similarity(&sessions[i], &sessions[j]),
                    min_session_confidence(&sessions[i], &sessions[j]),
                    pair_evidence(&sessions[i], &sessions[j], pressure)
                        .map_or(0.0, |edge| edge.score),
                ));
            }
        }
        lines.join("\n")
    }

    fn archive(records: &[LiveSessionRecord], now_ms: u64) -> VecDeque<HistoricalSignature> {
        let mut signatures = VecDeque::new();
        for record in records {
            archive_signatures(record, now_ms, &mut signatures);
        }
        signatures
    }

    fn assert_detected(
        label: &str,
        records: Vec<LiveSessionRecord>,
        history: VecDeque<HistoricalSignature>,
        pressure: f64,
        now_ms: u64,
        min_evictions: usize,
    ) {
        let (evaluation, debug) = evaluate_case(records, history, pressure, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{label}\n{debug}");
        assert!(
            evaluation.evicted_session_ids.len() >= min_evictions,
            "{label}\n{debug}"
        );
    }

    fn assert_clean(
        label: &str,
        records: Vec<LiveSessionRecord>,
        history: VecDeque<HistoricalSignature>,
        pressure: f64,
        now_ms: u64,
    ) {
        let (evaluation, debug) = evaluate_case(records, history, pressure, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 0, "{label}\n{debug}");
        assert!(
            evaluation.evicted_session_ids.is_empty(),
            "{label}\n{debug}"
        );
    }

    fn fixed_trace(now_ms: u64, events: &[(u64, &[u8])]) -> Trace {
        events
            .iter()
            .map(|(offset, bytes)| (now_ms - *offset, bytes.to_vec()))
            .collect()
    }

    fn replay_trace(now_ms: u64, seed: u64) -> Trace {
        vec![
            (now_ms - 220_000 + seed * 3, b"\x1b[A".to_vec()),
            (now_ms - 205_000 + seed * 5, b"d".to_vec()),
            (now_ms - 191_000 + seed * 2, b" ".to_vec()),
            (now_ms - 170_000 + seed * 4, b"\x1b[C".to_vec()),
            (now_ms - 151_000 + seed * 3, b"w".to_vec()),
            (now_ms - 122_000 + seed * 4, b"\r".to_vec()),
            (now_ms - 96_000 + seed * 2, b"a".to_vec()),
            (now_ms - 72_000 + seed * 3, b"s".to_vec()),
            (now_ms - 44_000 + seed * 2, b"\x1b[B".to_vec()),
            (now_ms - 19_000 + seed * 4, b"1".to_vec()),
        ]
    }

    fn jittered_random_trace(now_ms: u64, family: u64, variant: u64) -> Trace {
        const ALPHABET: &[u8] = b"asdfjkl;12";
        let mut at_ms = now_ms - 250_000;
        (0..8)
            .map(|step| {
                let noise = splitmix64(family ^ ((variant + 1) << 8) ^ step as u64);
                at_ms += 18_000 + family * 1_300 + (noise % 24_000) + variant * 90;
                let token = ALPHABET[((noise >> 8) as usize + step) % ALPHABET.len()];
                (at_ms, vec![token])
            })
            .collect()
    }

    fn burst_macro_trace(now_ms: u64, family: u64, variant: u64) -> Trace {
        const BURSTS: &[&[&[u8]]] = &[
            &[b"\x1b[A", b" ", b"\r"],
            &[b"\x1b[C", b"\x1b[C", b"z"],
            &[b"\x1b[B", b"x", b"1"],
        ];
        let mut anchor = now_ms - 210_000;
        let mut events = Vec::new();
        for (burst_idx, burst) in BURSTS.iter().enumerate() {
            anchor += 52_000 + family * 1_400 + variant * 110 + burst_idx as u64 * 1_700;
            for (event_idx, bytes) in burst.iter().enumerate() {
                events.push((
                    anchor + event_idx as u64 * (90 + variant * 3),
                    bytes.to_vec(),
                ));
            }
        }
        events
    }

    fn noise_injected_replay_trace(now_ms: u64, seed: u64) -> Trace {
        const VARIANTS: &[&[&[u8]]] = &[
            &[b"\x1b[A", b"w"],
            &[b"d", b"\x1b[C"],
            &[b" ", b"\r"],
            &[b"\x1b[C", b"e"],
            &[b"w", b"\x1b[A"],
            &[b"\r", b" "],
            &[b"a", b"\x1b[D"],
            &[b"s", b"x"],
            &[b"\x1b[B", b"q"],
            &[b"1", b"2"],
        ];
        let replay = replay_trace(now_ms, seed);
        replay
            .iter()
            .enumerate()
            .map(|(idx, (at_ms, _))| {
                let noise = splitmix64(seed ^ (idx as u64).wrapping_mul(0x9E37_79B9));
                let payload = VARIANTS[idx][((noise >> 8) & 1) as usize];
                (
                    (((*at_ms as i64) + (noise % 1_400) as i64 - 700).max(0)) as u64,
                    payload.to_vec(),
                )
            })
            .collect()
    }

    fn human_random_trace(now_ms: u64, seed: u64) -> Trace {
        const PALETTE: &[&[u8]] = &[
            b"w", b"a", b"s", b"d", b"q", b"e", b"r", b" ", b"\r", b"\x1b[A", b"\x1b[B", b"\x1b[C",
            b"\x1b[D", b"1", b"2", b"3", b"z", b"x",
        ];
        let mut at_ms = now_ms - 210_000 - seed * 1_500;
        (0..14)
            .map(|step| {
                let noise =
                    splitmix64(seed.rotate_left(9) ^ (step as u64).wrapping_mul(0xBF58_476D));
                at_ms += 3_500 + (noise % 19_000) + (step as u64 % 3) * 1_700;
                (
                    at_ms,
                    PALETTE[((noise >> 11) as usize + step * 3) % PALETTE.len()].to_vec(),
                )
            })
            .collect()
    }

    fn rich_token(seed: u64) -> Vec<u8> {
        const SINGLE_BYTES: &[u8] =
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !?#@%&*()=+/-";
        const ESCAPES: &[&[u8]] = &[b"\r", b"\t", b"\x1b[A", b"\x1b[B", b"\x1b[C", b"\x1b[D"];
        let idx = (seed % (SINGLE_BYTES.len() + ESCAPES.len()) as u64) as usize;
        if idx < SINGLE_BYTES.len() {
            vec![SINGLE_BYTES[idx]]
        } else {
            ESCAPES[idx - SINGLE_BYTES.len()].to_vec()
        }
    }

    fn full_alphabet_bot_trace(now_ms: u64, family: u64, variant: u64) -> Trace {
        let mut at_ms = now_ms - 320_000 + variant * 37;
        let mut events = Vec::new();
        for step in 0..24 {
            let noise = splitmix64(
                family.rotate_left(7)
                    ^ (variant + 1).wrapping_mul(0x9E37_79B9)
                    ^ (step as u64).wrapping_mul(0xBF58_476D),
            );
            let lane = (step % 4) as u64;
            at_ms += 4_800 + family * 420 + lane * 1_350 + (noise % 4_600);
            let token_seed =
                family * 113 + lane * 29 + (step / 3) as u64 * 41 + ((noise >> 12) & 0x0f);
            events.push((at_ms, rich_token(token_seed)));
            if step % 6 == 5 {
                events.push((at_ms + 18 + (noise % 27), rich_token(token_seed ^ 0x55)));
            }
        }
        events
    }

    fn full_alphabet_human_trace(now_ms: u64, seed: u64) -> Trace {
        let mut at_ms = now_ms - 260_000 - seed * 1_200;
        let mut events = Vec::new();
        for step in 0..30 {
            let noise = splitmix64(seed.rotate_left(11) ^ (step as u64).wrapping_mul(0x94D0_49BB));
            at_ms += [1_300, 4_800, 9_200, 2_200, 14_500][step % 5] + (noise % 8_200);
            events.push((at_ms, rich_token(seed ^ noise ^ ((step as u64) << 6))));
            if step % 7 == 2 {
                events.push((
                    at_ms + 120 + (noise % 480),
                    rich_token(seed ^ noise.rotate_left(9) ^ 0xA5A5),
                ));
            }
            if step % 9 == 4 {
                events.push((at_ms + 35 + (noise % 90), b"\x08".to_vec()));
            }
        }
        events
    }

    fn full_alphabet_sparse_keepalive_trace(now_ms: u64, family: u64, variant: u64) -> Trace {
        let mut at_ms = now_ms - 310_000 + variant * 33;
        (0..6)
            .map(|step| {
                let noise =
                    splitmix64(family.rotate_left(5) ^ variant.rotate_left(17) ^ step as u64);
                at_ms += 58_000 + family * 190 + (noise % 2_700);
                (
                    at_ms,
                    rich_token(family * 31 + variant * 7 + step as u64 * 13),
                )
            })
            .collect()
    }

    fn mouse_sgr_motion(x: u16, y: u16) -> Vec<u8> {
        format!("\x1b[<35;{};{}M", x, y).into_bytes()
    }

    fn human_mouse_hover_trace(now_ms: u64, seed: u64) -> Trace {
        let mut at_ms = now_ms - 110_000 - seed * 800;
        let (mut x, mut y) = (18 + (seed as u16 % 9), 10 + (seed as u16 % 7));
        let mut events = Vec::new();
        for step in 0..56 {
            let noise = splitmix64(seed.rotate_left(5) ^ (step as u64).wrapping_mul(0x94D0_49BB));
            at_ms += 42 + (noise % 37) + (step as u64 % 5) * 6;
            x = (x as i16 + ((noise & 0x0f) as i16) - 7).clamp(2, 120) as u16;
            y = (y as i16 + (((noise >> 4) & 0x0f) as i16) - 7).clamp(2, 50) as u16;
            events.push((at_ms, mouse_sgr_motion(x, y)));
            if step % 11 == 0 {
                events.push((at_ms + 14, b" ".to_vec()));
            }
            if step % 17 == 4 {
                events.push((at_ms + 27, b"\x1b[C".to_vec()));
            }
        }
        events
    }

    fn mouse_sweep_bot_trace(now_ms: u64, family: u64, variant: u64) -> Trace {
        let mut at_ms = now_ms - 120_000 + variant * 9;
        let mut events = Vec::new();
        for lap in 0..5 {
            let (base_x, base_y) = (12 + family as u16 * 3, 8 + ((variant + lap) % 3) as u16 * 2);
            for step in 0..14 {
                at_ms += 28 + (lap % 2) * 2;
                events.push((
                    at_ms,
                    mouse_sgr_motion(base_x + step as u16 * 3, base_y + ((step / 3) % 4) as u16),
                ));
            }
            at_ms += 1_200 + family * 15;
            events.push((at_ms, b"\r".to_vec()));
        }
        events
    }

    #[test]
    fn replay_evidence_respects_pressure_and_cluster_size() {
        let now_ms = 900_000;
        let history = archive(
            &swarm(6, 1, REPLAY_OUTPUTS, |idx| {
                (
                    ipv4((idx + 1) as u8),
                    transport(idx),
                    now_ms - 600_000 - idx as u64 * 4_000,
                    replay_trace(now_ms - 300_000, idx as u64),
                )
            }),
            now_ms - 240_000,
        );

        assert_detected(
            "replay cohort under pressure",
            swarm(6, 100, REPLAY_OUTPUTS, |idx| {
                (
                    ipv4_in(100, 10 + idx as u8),
                    transport(idx + 1),
                    now_ms - 260_000,
                    replay_trace(now_ms, (5 - idx) as u64),
                )
            }),
            history.clone(),
            0.96,
            now_ms,
            6,
        );

        assert_clean(
            "replay cohort at low pressure",
            swarm(4, 200, &[12_000, 44_000, 81_000, 126_000, 173_000], |idx| {
                (
                    ipv4((idx + 20) as u8),
                    Transport::Web,
                    now_ms - 240_000,
                    replay_trace(now_ms, idx as u64),
                )
            }),
            history,
            0.20,
            now_ms,
        );

        assert_clean(
            "singleton replay match",
            vec![session(
                500,
                ipv4(33),
                Transport::Web,
                now_ms - 220_000,
                replay_trace(now_ms, 0),
                &[10_000, 40_000, 80_000, 120_000, 160_000],
            )],
            archive(
                &[session(
                    1,
                    ipv4(1),
                    Transport::Ssh,
                    now_ms - 400_000,
                    replay_trace(now_ms - 200_000, 0),
                    &[10_000, 40_000, 80_000, 120_000, 160_000],
                )],
                now_ms - 180_000,
            ),
            0.98,
            now_ms,
        );
    }

    #[test]
    fn bot_families_are_detected_under_pressure() {
        let now_ms = 1_600_000;
        assert_detected(
            "sparse keepalive",
            swarm(
                6,
                1,
                &[20_000, 80_000, 140_000, 200_000, 260_000, 320_000],
                |idx| {
                    (
                        ipv4((idx + 1) as u8),
                        Transport::Ssh,
                        now_ms - 6 * 60_000,
                        vec![
                            (now_ms - 300_000 + idx as u64 * 40, b"a".to_vec()),
                            (now_ms - 240_500 + idx as u64 * 35, b"9".to_vec()),
                            (now_ms - 180_250 + idx as u64 * 45, b"b".to_vec()),
                            (now_ms - 120_400 + idx as u64 * 30, b"1".to_vec()),
                            (now_ms - 60_350 + idx as u64 * 50, b"c".to_vec()),
                        ],
                    )
                },
            ),
            VecDeque::new(),
            0.92,
            now_ms,
            4,
        );

        assert_detected(
            "random interval keypresses",
            swarm(8, 100, BOT_OUTPUTS, |idx| {
                (
                    ipv4_in(100, 30 + idx as u8),
                    transport(idx),
                    now_ms - 280_000,
                    jittered_random_trace(now_ms, 3, idx as u64),
                )
            }),
            VecDeque::new(),
            0.91,
            now_ms,
            4,
        );

        assert_detected(
            "burst macro family",
            swarm(
                6,
                200,
                &[
                    8_000, 24_000, 41_000, 66_000, 91_000, 117_000, 143_000, 168_000, 194_000,
                ],
                |idx| {
                    (
                        ipv4((idx + 50) as u8),
                        Transport::Ssh,
                        now_ms - 240_000,
                        burst_macro_trace(now_ms, 5, idx as u64),
                    )
                },
            ),
            VecDeque::new(),
            0.88,
            now_ms,
            2,
        );

        let history = archive(
            &swarm(5, 300, REPLAY_OUTPUTS, |idx| {
                (
                    ipv4_in(60 + idx as u8, 20 + idx as u8),
                    transport(idx),
                    now_ms - 700_000 - idx as u64 * 5_500,
                    noise_injected_replay_trace(now_ms - 330_000, idx as u64),
                )
            }),
            now_ms - 260_000,
        );
        assert_detected(
            "noise injected replay",
            swarm(
                6,
                400,
                &[11_000, 36_000, 72_000, 107_000, 143_000, 179_000, 214_000],
                |idx| {
                    (
                        ipv4_in(90 + idx as u8, 30 + idx as u8),
                        transport(idx + 1),
                        now_ms - 300_000 + idx as u64 * 700,
                        noise_injected_replay_trace(now_ms, (idx % 5) as u64),
                    )
                },
            ),
            history,
            0.95,
            now_ms,
            4,
        );

        assert_detected(
            "mouse sweep bot",
            swarm(
                6,
                500,
                &[
                    9_000, 31_000, 54_000, 76_000, 99_000, 121_000, 145_000, 167_000,
                ],
                |idx| {
                    (
                        ipv4_in(180 + idx as u8, 80 + idx as u8),
                        transport(idx),
                        now_ms - 200_000 + idx as u64 * 500,
                        mouse_sweep_bot_trace(now_ms, 4, idx as u64),
                    )
                },
            ),
            VecDeque::new(),
            0.95,
            now_ms,
            4,
        );

        assert_detected(
            "full alphabet randomized bot",
            swarm(
                10,
                600,
                &[
                    10_000, 33_000, 57_000, 81_000, 108_000, 136_000, 163_000, 191_000,
                ],
                |idx| {
                    (
                        ipv4_in(200 + idx as u8, 20 + idx as u8),
                        transport(idx),
                        now_ms - 280_000 + idx as u64 * 700,
                        full_alphabet_bot_trace(now_ms, 9, idx as u64),
                    )
                },
            ),
            VecDeque::new(),
            0.95,
            now_ms,
            4,
        );

        assert_detected(
            "full alphabet sparse keepalive",
            swarm(
                8,
                700,
                &[18_000, 77_000, 136_000, 194_000, 253_000, 311_000],
                |idx| {
                    (
                        ipv4_in(220 + idx as u8, 40 + idx as u8),
                        Transport::Ssh,
                        now_ms - 7 * 60_000,
                        full_alphabet_sparse_keepalive_trace(now_ms, 6, idx as u64),
                    )
                },
            ),
            VecDeque::new(),
            0.94,
            now_ms,
            4,
        );
    }

    #[test]
    fn human_families_stay_clean_under_pressure() {
        let now_ms = 1_680_000;
        assert_clean(
            "human mouse hover",
            swarm(
                4,
                1,
                &[
                    6_000, 24_000, 43_000, 66_000, 91_000, 117_000, 142_000, 168_000,
                ],
                |idx| {
                    (
                        ipv4_in(150 + idx as u8, 60 + idx as u8),
                        transport(idx),
                        now_ms - 180_000 - idx as u64 * 2_000,
                        human_mouse_hover_trace(now_ms, 200 + idx as u64 * 13),
                    )
                },
            ),
            VecDeque::new(),
            0.96,
            now_ms,
        );

        assert_clean(
            "randomized humans",
            swarm(
                6,
                100,
                &[
                    7_000, 24_000, 43_000, 61_000, 84_000, 102_000, 129_000, 151_000, 176_000,
                ],
                |idx| {
                    (
                        ipv4_in(120 + idx as u8, 40 + idx as u8),
                        transport(idx),
                        now_ms - 240_000 - idx as u64 * 3_000,
                        human_random_trace(now_ms, 100 + idx as u64 * 17),
                    )
                },
            ),
            VecDeque::new(),
            0.97,
            now_ms,
        );

        assert_clean(
            "full alphabet humans",
            swarm(8, 200, HUMAN_OUTPUTS, |idx| {
                (
                    ipv4_in(240 + idx as u8, 60 + idx as u8),
                    transport(idx),
                    now_ms - 260_000 - idx as u64 * 1_500,
                    full_alphabet_human_trace(now_ms, 300 + idx as u64 * 23),
                )
            }),
            VecDeque::new(),
            0.97,
            now_ms,
        );

        assert_clean(
            "mixed active humans",
            vec![
                session(
                    1,
                    ipv4(1),
                    Transport::Ssh,
                    now_ms - 150_000,
                    fixed_trace(
                        now_ms,
                        &[
                            (145_000, b"\x1b[A"),
                            (141_200, b"d"),
                            (134_000, b" "),
                            (121_000, b"\x1b[C"),
                            (108_000, b"\r"),
                            (92_500, b"a"),
                            (76_000, b"s"),
                            (51_500, b"\x1b[B"),
                            (27_000, b"1"),
                            (9_000, b"\x1b[D"),
                        ],
                    ),
                    &[5_000, 22_000, 39_000, 61_000, 79_000, 103_000, 129_000],
                ),
                session(
                    2,
                    ipv4(2),
                    Transport::Web,
                    now_ms - 155_000,
                    fixed_trace(
                        now_ms,
                        &[
                            (147_000, b"w"),
                            (139_000, b"\x1b[D"),
                            (131_000, b" "),
                            (118_000, b"e"),
                            (97_500, b"\x1b[A"),
                            (88_000, b"q"),
                            (63_000, b"r"),
                            (45_000, b"\r"),
                            (24_000, b"2"),
                            (6_000, b"\x1b[C"),
                        ],
                    ),
                    &[8_000, 28_000, 46_000, 68_000, 90_000, 114_000, 136_000],
                ),
                session(
                    3,
                    ipv4(3),
                    Transport::Ssh,
                    now_ms - 160_000,
                    fixed_trace(
                        now_ms,
                        &[
                            (149_000, b"\x1b[C"),
                            (144_500, b"z"),
                            (132_000, b"x"),
                            (116_000, b" "),
                            (100_000, b"\x1b[A"),
                            (78_000, b"c"),
                            (57_000, b"v"),
                            (34_000, b"\r"),
                            (17_000, b"\x1b[B"),
                            (4_000, b"3"),
                        ],
                    ),
                    &[11_000, 32_000, 54_000, 72_000, 93_000, 119_000, 143_000],
                ),
            ],
            VecDeque::new(),
            0.94,
            now_ms,
        );
    }

    #[test]
    fn suspicious_clusters_are_fully_evicted() {
        let now_ms = 800_000;
        let history = archive(
            &swarm(6, 1, REPLAY_OUTPUTS, |idx| {
                (
                    ipv4((idx + 1) as u8),
                    transport(idx),
                    now_ms - 620_000 - idx as u64 * 3_000,
                    replay_trace(now_ms - 250_000, (idx % 3) as u64),
                )
            }),
            now_ms - 210_000,
        );
        let records = swarm(
            5,
            100,
            &[9_000, 36_000, 74_000, 109_000, 145_000, 179_000, 214_000],
            |idx| {
                (
                    ipv4((40 + idx) as u8),
                    Transport::Ssh,
                    now_ms - 300_000 + idx as u64 * 40_000,
                    replay_trace(now_ms, (idx % 3) as u64),
                )
            },
        );
        let (evaluation, debug) = evaluate_case(records, history, 0.95, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert_eq!(evaluation.evicted_session_ids.len(), 5, "{debug}");
        assert!(evaluation.evicted_session_ids.contains(&100), "{debug}");
    }
}
