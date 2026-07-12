// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

//! Vulkan-backed execution for selected packed relation operators.

use fast_telemetry::{Counter, ExportMetrics, Histogram};
use mica_relation_kernel::{
    AccelerationDecline, AccelerationOutcome, MembershipSelection, RelationAccelerator,
};
use mica_var::{Identity, Value, ValueKind};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak, mpsc};
use std::time::{Duration, Instant};
use wgpu::util::DeviceExt;

const WORKGROUP_SIZE: u32 = 256;
const CACHE_SHARDS: usize = 64;
const METRIC_SHARDS: usize = 64;

static METRICS: LazyLock<RelationWgpuMetrics> =
    LazyLock::new(|| RelationWgpuMetrics::new(METRIC_SHARDS));

const MEMBERSHIP_SHADER: &str = r#"
struct Params {
    left_len: u32,
    right_len: u32,
    _padding_0: u32,
    _padding_1: u32,
}

@group(0) @binding(0)
var<storage, read> left_rows: array<u64>;

@group(0) @binding(1)
var<storage, read> right_rows: array<u64>;

@group(0) @binding(2)
var<storage, read_write> matches: array<u32>;

@group(0) @binding(3)
var<uniform> params: Params;

@compute @workgroup_size(256)
fn membership(@builtin(global_invocation_id) invocation: vec3<u32>) {
    let row = invocation.x;
    if row >= params.left_len {
        return;
    }

    let probe = left_rows[row];
    var lower = 0u;
    var upper = params.right_len;
    loop {
        if lower >= upper {
            break;
        }
        let middle = lower + ((upper - lower) / 2u);
        let candidate = right_rows[middle];
        if candidate < probe {
            lower = middle + 1u;
        } else {
            upper = middle;
        }
    }
    matches[row] = select(0u, 1u, lower < params.right_len && right_rows[lower] == probe);
}
"#;

#[derive(Clone, Copy, Debug, Default)]
pub struct WgpuAcceleratorOptions {
    pub allow_software_adapter: bool,
}

#[derive(Debug)]
pub struct WgpuInitializationError {
    message: String,
}

impl WgpuInitializationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for WgpuInitializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WgpuInitializationError {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BufferMode {
    StagedReadback,
    SharedMappable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueEncoding {
    Int,
    Identity,
    Float,
    Dictionary,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ColumnLayout {
    RowOrder,
    SortedUnique,
}

struct EncodedColumn {
    encoding: ValueEncoding,
    buffer: wgpu::Buffer,
    len: usize,
}

struct CachedColumn {
    source: Weak<[Value]>,
    encoded: Arc<EncodedColumn>,
}

struct EncodedPair {
    left: Arc<EncodedColumn>,
    right: Arc<EncodedColumn>,
}

struct CachedPair {
    left_source: Weak<[Value]>,
    right_source: Weak<[Value]>,
    encoded: Arc<EncodedPair>,
}

struct OutputBuffers {
    output: wgpu::Buffer,
    readback: Option<wgpu::Buffer>,
    size: u64,
}

#[repr(align(128))]
struct GpuAdmission {
    occupied: AtomicBool,
}

#[repr(align(128))]
struct GpuAvailability {
    enabled: AtomicBool,
}

struct GpuPermit<'a> {
    admission: &'a GpuAdmission,
}

impl GpuAdmission {
    fn try_acquire(&self) -> Option<GpuPermit<'_>> {
        self.occupied
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| GpuPermit { admission: self })
    }
}

impl Drop for GpuPermit<'_> {
    fn drop(&mut self) {
        self.admission.occupied.store(false, Ordering::Release);
    }
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_relation_wgpu"]
pub struct RelationWgpuMetrics {
    #[help = "GPU initialization failures that left relation acceleration unavailable"]
    pub initialization_failures: Counter,

    #[help = "Encoded column cache hits"]
    pub encoded_column_cache_hits: Counter,

    #[help = "Encoded column cache misses"]
    pub encoded_column_cache_misses: Counter,

    #[help = "Encoded column construction duration in microseconds"]
    pub encoded_column_duration_us: Histogram,

    #[help = "GPU membership execution duration in microseconds"]
    pub membership_duration_us: Histogram,

    #[help = "GPU membership operations declined because the device was occupied"]
    pub membership_busy: Counter,

    #[help = "GPU device failures that disabled relation acceleration"]
    pub device_failures: Counter,
}

impl RelationWgpuMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            initialization_failures: Counter::new(shard_count),
            encoded_column_cache_hits: Counter::new(shard_count),
            encoded_column_cache_misses: Counter::new(shard_count),
            encoded_column_duration_us: Histogram::with_latency_buckets(shard_count),
            membership_duration_us: Histogram::with_latency_buckets(shard_count),
            membership_busy: Counter::new(shard_count),
            device_failures: Counter::new(shard_count),
        }
    }
}

pub fn metrics() -> &'static RelationWgpuMetrics {
    &METRICS
}

pub struct WgpuAccelerator {
    adapter_name: String,
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    buffer_mode: BufferMode,
    max_left_rows: usize,
    max_right_rows: usize,
    admission: GpuAdmission,
    availability: GpuAvailability,
    column_cache: [Mutex<HashMap<(usize, ColumnLayout), CachedColumn>>; CACHE_SHARDS],
    dictionary_cache: [Mutex<HashMap<(usize, usize), CachedPair>>; CACHE_SHARDS],
    output_pool: Mutex<HashMap<u64, Vec<OutputBuffers>>>,
}

impl WgpuAccelerator {
    pub fn new(options: WgpuAcceleratorOptions) -> Result<Self, WgpuInitializationError> {
        if cfg!(target_endian = "big") {
            return Err(WgpuInitializationError::new(
                "the wgpu relation accelerator requires a little-endian host",
            ));
        }
        let mut instance_descriptor = wgpu::InstanceDescriptor::new_without_display_handle();
        instance_descriptor.backends = wgpu::Backends::VULKAN;
        let instance = wgpu::Instance::new(instance_descriptor);
        let adapters = pollster::block_on(instance.enumerate_adapters(wgpu::Backends::VULKAN));
        let adapter = adapters
            .into_iter()
            .find(|adapter| {
                options.allow_software_adapter
                    || !matches!(adapter.get_info().device_type, wgpu::DeviceType::Cpu)
            })
            .ok_or_else(|| {
                WgpuInitializationError::new(if options.allow_software_adapter {
                    "no Vulkan adapter is available"
                } else {
                    "no hardware Vulkan adapter is available"
                })
            })?;
        let adapter_info = adapter.get_info();
        let supported = adapter.features();
        let required = wgpu::Features::SHADER_INT64;
        if !supported.contains(required) {
            return Err(WgpuInitializationError::new(format!(
                "Vulkan adapter {:?} lacks shaderInt64",
                adapter_info.name
            )));
        }
        let shared_mappable = supported.contains(wgpu::Features::MAPPABLE_PRIMARY_BUFFERS);
        let requested_features =
            required | supported.intersection(wgpu::Features::MAPPABLE_PRIMARY_BUFFERS);
        let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
            label: Some("mica-relation-wgpu-device"),
            required_features: requested_features,
            required_limits: wgpu::Limits::default().using_resolution(adapter.limits()),
            ..Default::default()
        }))
        .map_err(|error| {
            WgpuInitializationError::new(format!(
                "failed to request Vulkan device {:?}: {error}",
                adapter_info.name
            ))
        })?;
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("mica-relation-membership-shader"),
            source: wgpu::ShaderSource::Wgsl(MEMBERSHIP_SHADER.into()),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("mica-relation-membership-pipeline"),
            layout: None,
            module: &shader,
            entry_point: Some("membership"),
            compilation_options: Default::default(),
            cache: None,
        });
        let limits = device.limits();
        let max_storage_bytes = limits.max_storage_buffer_binding_size as usize;
        let max_dispatch_rows =
            limits.max_compute_workgroups_per_dimension as usize * WORKGROUP_SIZE as usize;
        Ok(Self {
            adapter_name: adapter_info.name,
            device,
            queue,
            pipeline,
            buffer_mode: if shared_mappable {
                BufferMode::SharedMappable
            } else {
                BufferMode::StagedReadback
            },
            max_left_rows: max_dispatch_rows
                .min(max_storage_bytes / size_of::<u64>())
                .min(max_storage_bytes / size_of::<u32>()),
            max_right_rows: (max_storage_bytes / size_of::<u64>()).min(u32::MAX as usize),
            admission: GpuAdmission {
                occupied: AtomicBool::new(false),
            },
            availability: GpuAvailability {
                enabled: AtomicBool::new(true),
            },
            column_cache: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            dictionary_cache: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            output_pool: Mutex::new(HashMap::new()),
        })
    }

    pub fn adapter_name(&self) -> &str {
        &self.adapter_name
    }

    pub fn uses_shared_mappable_buffers(&self) -> bool {
        self.buffer_mode == BufferMode::SharedMappable
    }

    fn try_acquire(&self) -> Option<GpuPermit<'_>> {
        self.admission.try_acquire()
    }

    fn encoded_column(
        &self,
        source: &Arc<[Value]>,
        layout: ColumnLayout,
    ) -> Option<Arc<EncodedColumn>> {
        let key = (source.as_ptr() as usize, layout);
        let shard = key.0 % CACHE_SHARDS;
        {
            let mut cache = self.column_cache[shard].lock().unwrap();
            cache.retain(|_, cached| cached.source.strong_count() != 0);
            if let Some(cached) = cache.get(&key)
                && Weak::ptr_eq(&cached.source, &Arc::downgrade(source))
            {
                metrics().encoded_column_cache_hits.inc();
                return Some(Arc::clone(&cached.encoded));
            }
        }

        metrics().encoded_column_cache_misses.inc();
        let started = Instant::now();
        let encoding = detect_encoding(source, &[])?;
        let mut values = encode_column(source, encoding)?;
        if layout == ColumnLayout::SortedUnique {
            values.sort_unstable();
            values.dedup();
        }
        let encoded = self.create_encoded_column(encoding, &values);
        self.column_cache[shard].lock().unwrap().insert(
            key,
            CachedColumn {
                source: Arc::downgrade(source),
                encoded: Arc::clone(&encoded),
            },
        );
        metrics()
            .encoded_column_duration_us
            .record(duration_us(started.elapsed()));
        Some(encoded)
    }

    fn encoded_string_pair(
        &self,
        left: &Arc<[Value]>,
        right: &Arc<[Value]>,
    ) -> Option<Arc<EncodedPair>> {
        if !left
            .iter()
            .chain(right.iter())
            .all(|value| value.kind() == ValueKind::String)
        {
            return None;
        }
        let key = (left.as_ptr() as usize, right.as_ptr() as usize);
        let shard = (key.0 ^ key.1.rotate_left(17)) % CACHE_SHARDS;
        {
            let mut cache = self.dictionary_cache[shard].lock().unwrap();
            cache.retain(|_, cached| {
                cached.left_source.strong_count() != 0 && cached.right_source.strong_count() != 0
            });
            if let Some(cached) = cache.get(&key)
                && Weak::ptr_eq(&cached.left_source, &Arc::downgrade(left))
                && Weak::ptr_eq(&cached.right_source, &Arc::downgrade(right))
            {
                metrics().encoded_column_cache_hits.inc();
                return Some(Arc::clone(&cached.encoded));
            }
        }

        metrics().encoded_column_cache_misses.inc();
        let started = Instant::now();
        let mut dictionary = left
            .iter()
            .chain(right.iter())
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        dictionary.sort_unstable();
        let codes = dictionary
            .iter()
            .cloned()
            .enumerate()
            .map(|(code, value)| (value, code as u64))
            .collect::<HashMap<_, _>>();
        let left_codes = left
            .iter()
            .map(|value| codes.get(value).copied())
            .collect::<Option<Vec<_>>>()?;
        let mut right_codes = right
            .iter()
            .map(|value| codes.get(value).copied())
            .collect::<Option<Vec<_>>>()?;
        right_codes.sort_unstable();
        right_codes.dedup();
        let encoded = Arc::new(EncodedPair {
            left: self.create_encoded_column(ValueEncoding::Dictionary, &left_codes),
            right: self.create_encoded_column(ValueEncoding::Dictionary, &right_codes),
        });
        self.dictionary_cache[shard].lock().unwrap().insert(
            key,
            CachedPair {
                left_source: Arc::downgrade(left),
                right_source: Arc::downgrade(right),
                encoded: Arc::clone(&encoded),
            },
        );
        metrics()
            .encoded_column_duration_us
            .record(duration_us(started.elapsed()));
        Some(encoded)
    }

    fn create_encoded_column(&self, encoding: ValueEncoding, values: &[u64]) -> Arc<EncodedColumn> {
        let input_usage = match self.buffer_mode {
            BufferMode::StagedReadback => wgpu::BufferUsages::STORAGE,
            BufferMode::SharedMappable => {
                wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::MAP_WRITE
            }
        };
        Arc::new(EncodedColumn {
            encoding,
            buffer: create_u64_buffer(
                &self.device,
                "mica-relation-encoded-column",
                values,
                input_usage,
            ),
            len: values.len(),
        })
    }

    fn acquire_output_buffers(&self, row_count: usize) -> OutputBuffers {
        let required = (row_count * size_of::<u32>()) as u64;
        let size = required.next_power_of_two();
        if let Some(buffers) = self
            .output_pool
            .lock()
            .unwrap()
            .get_mut(&size)
            .and_then(Vec::pop)
        {
            return buffers;
        }
        let output_usage = match self.buffer_mode {
            BufferMode::StagedReadback => {
                wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC
            }
            BufferMode::SharedMappable => {
                wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::MAP_READ
            }
        };
        let output = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("mica-relation-membership-output"),
            size,
            usage: output_usage,
            mapped_at_creation: false,
        });
        let readback = (self.buffer_mode == BufferMode::StagedReadback).then(|| {
            self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("mica-relation-membership-readback"),
                size,
                usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            })
        });
        OutputBuffers {
            output,
            readback,
            size,
        }
    }

    fn release_output_buffers(&self, buffers: OutputBuffers) {
        self.output_pool
            .lock()
            .unwrap()
            .entry(buffers.size)
            .or_default()
            .push(buffers);
    }

    fn execute_membership(
        &self,
        left: &EncodedColumn,
        right: &EncodedColumn,
        keep_matches: bool,
    ) -> Result<Vec<usize>, String> {
        if left.len == 0 {
            return Ok(Vec::new());
        }
        if right.len == 0 {
            return Ok(if keep_matches {
                Vec::new()
            } else {
                (0..left.len).collect()
            });
        }
        let output_buffers = self.acquire_output_buffers(left.len);
        let output_size = (left.len * size_of::<u32>()) as u64;
        let params = [left.len as u32, right.len as u32, 0, 0];
        let params_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("mica-relation-membership-params"),
                contents: u32_bytes(&params),
                usage: wgpu::BufferUsages::UNIFORM,
            });
        let layout = self.pipeline.get_bind_group_layout(0);
        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("mica-relation-membership-bind-group"),
            layout: &layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: left.buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: right.buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: output_buffers.output.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: params_buffer.as_entire_binding(),
                },
            ],
        });
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("mica-relation-membership-command-encoder"),
            });
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("mica-relation-membership-compute-pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups((left.len as u32).div_ceil(WORKGROUP_SIZE), 1, 1);
        }
        let output_readback = output_buffers
            .readback
            .as_ref()
            .unwrap_or(&output_buffers.output);
        if let Some(readback) = &output_buffers.readback {
            encoder.copy_buffer_to_buffer(&output_buffers.output, 0, readback, 0, output_size);
        }
        self.queue.submit([encoder.finish()]);

        let output_slice = output_readback.slice(..output_size);
        let output_result = map_for_read(&output_slice);
        self.device
            .poll(wgpu::PollType::wait_indefinitely())
            .map_err(|error| format!("failed while waiting for Vulkan membership work: {error}"))?;
        output_result
            .recv()
            .map_err(|_| "membership mapping callback was dropped".to_owned())?
            .map_err(|error| format!("failed to map membership output: {error}"))?;
        let output_view = output_slice
            .get_mapped_range()
            .map_err(|error| format!("failed to access mapped membership output: {error}"))?;
        let selected = output_view
            .chunks_exact(size_of::<u32>())
            .enumerate()
            .filter_map(|(row, flag)| ((read_u32(flag) != 0) == keep_matches).then_some(row))
            .collect();
        drop(output_view);
        output_readback.unmap();
        self.release_output_buffers(output_buffers);
        Ok(selected)
    }
}

impl RelationAccelerator for WgpuAccelerator {
    fn select_membership(&self, selection: MembershipSelection<'_>) -> AccelerationOutcome {
        if selection.left.is_empty() {
            return AccelerationOutcome::Selected(Vec::new());
        }
        if selection.right.is_empty() {
            return AccelerationOutcome::Selected(if selection.keep_matches {
                Vec::new()
            } else {
                (0..selection.left.len()).collect()
            });
        }
        if selection.left.len() > self.max_left_rows || selection.right.len() > self.max_right_rows
        {
            return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedInput);
        }
        if !self.availability.enabled.load(Ordering::Acquire) {
            return AccelerationOutcome::Declined(AccelerationDecline::Unavailable);
        }
        let Some(_permit) = self.try_acquire() else {
            metrics().membership_busy.inc();
            return AccelerationOutcome::Declined(AccelerationDecline::Busy);
        };
        let (left, right) = if detect_encoding(selection.left, selection.right).is_some() {
            let Some(left) = self.encoded_column(selection.left, ColumnLayout::RowOrder) else {
                return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedDomain);
            };
            let Some(right) = self.encoded_column(selection.right, ColumnLayout::SortedUnique)
            else {
                return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedDomain);
            };
            if left.encoding != right.encoding {
                return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedDomain);
            }
            (left, right)
        } else {
            let Some(pair) = self.encoded_string_pair(selection.left, selection.right) else {
                return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedDomain);
            };
            (Arc::clone(&pair.left), Arc::clone(&pair.right))
        };
        let started = Instant::now();
        match self.execute_membership(&left, &right, selection.keep_matches) {
            Ok(selected) => {
                metrics()
                    .membership_duration_us
                    .record(duration_us(started.elapsed()));
                AccelerationOutcome::Selected(selected)
            }
            Err(_) => {
                self.availability.enabled.store(false, Ordering::Release);
                metrics().device_failures.inc();
                AccelerationOutcome::Declined(AccelerationDecline::Failed)
            }
        }
    }
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn detect_encoding(left: &[Value], right: &[Value]) -> Option<ValueEncoding> {
    let value = left.first().or_else(|| right.first())?;
    if value.as_int().is_some() {
        Some(ValueEncoding::Int)
    } else if value.as_identity().is_some() {
        Some(ValueEncoding::Identity)
    } else if value.as_float().is_some() {
        Some(ValueEncoding::Float)
    } else {
        None
    }
}

fn encode_column(values: &[Value], encoding: ValueEncoding) -> Option<Vec<u64>> {
    values
        .iter()
        .map(|value| encode_value(value, encoding))
        .collect()
}

fn encode_value(value: &Value, encoding: ValueEncoding) -> Option<u64> {
    match encoding {
        ValueEncoding::Int => value.as_int().map(|value| (value as u64) ^ (1 << 63)),
        ValueEncoding::Identity => value.as_identity().map(Identity::raw),
        ValueEncoding::Float => value.as_float().map(|value| {
            let bits = (value as f32).to_bits();
            if (bits & 0x8000_0000) != 0 {
                u64::from(!bits)
            } else {
                u64::from(bits ^ 0x8000_0000)
            }
        }),
        ValueEncoding::Dictionary => None,
    }
}

fn create_u64_buffer(
    device: &wgpu::Device,
    label: &str,
    values: &[u64],
    usage: wgpu::BufferUsages,
) -> wgpu::Buffer {
    let buffer = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some(label),
        size: std::mem::size_of_val(values) as u64,
        usage,
        mapped_at_creation: true,
    });
    buffer
        .slice(..)
        .get_mapped_range_mut()
        .expect("newly created input buffer should remain mapped")
        .copy_from_slice(u64_bytes(values));
    buffer.unmap();
    buffer
}

fn map_for_read(
    slice: &wgpu::BufferSlice<'_>,
) -> mpsc::Receiver<Result<(), wgpu::BufferAsyncError>> {
    let (sender, receiver) = mpsc::channel();
    slice.map_async(wgpu::MapMode::Read, move |result| {
        let _ = sender.send(result);
    });
    receiver
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_ne_bytes(bytes.try_into().unwrap())
}

fn u32_bytes(values: &[u32]) -> &[u8] {
    // SAFETY: Every `u32` byte pattern is valid, and the returned slice has the
    // same lifetime and exact byte extent as `values`.
    unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    }
}

fn u64_bytes(values: &[u64]) -> &[u8] {
    // SAFETY: Every `u64` byte pattern is valid, and the returned slice has the
    // same lifetime and exact byte extent as `values`.
    unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn fixed_width_encodings_preserve_value_order() {
        let cases = [
            (
                ValueEncoding::Int,
                vec![
                    Value::int(-7).unwrap(),
                    Value::int(0).unwrap(),
                    Value::int(9).unwrap(),
                ],
            ),
            (
                ValueEncoding::Identity,
                vec![
                    Value::identity(Identity::new(0).unwrap()),
                    Value::identity(Identity::new(3).unwrap()),
                    Value::identity(Identity::new(10).unwrap()),
                ],
            ),
            (
                ValueEncoding::Float,
                vec![Value::float(-7.5), Value::float(0.0), Value::float(9.25)],
            ),
        ];
        for (encoding, values) in cases {
            assert!(values.windows(2).all(|window| window[0] < window[1]));
            let encoded = encode_column(&values, encoding).unwrap();
            assert!(encoded.windows(2).all(|window| window[0] < window[1]));
        }
    }

    #[test]
    fn encoding_rejects_mixed_domains() {
        assert!(
            encode_column(
                &[Value::int(1).unwrap(), Value::float(2.0)],
                ValueEncoding::Int,
            )
            .is_none()
        );
    }

    #[test]
    fn gpu_admission_declines_instead_of_waiting() {
        let admission = GpuAdmission {
            occupied: AtomicBool::new(false),
        };
        let permit = admission.try_acquire().unwrap();
        assert!(admission.try_acquire().is_none());
        drop(permit);
        assert!(admission.try_acquire().is_some());
    }

    #[test]
    #[ignore = "requires a Vulkan adapter with shaderInt64"]
    fn hardware_membership_selects_original_row_indexes() {
        let accelerator = WgpuAccelerator::new(WgpuAcceleratorOptions::default()).unwrap();
        let left = Arc::from(
            [30, 10, 40, 10]
                .map(|value| Value::int(value).unwrap())
                .to_vec(),
        );
        let right = Arc::from([10, 20].map(|value| Value::int(value).unwrap()).to_vec());
        let cache_hits = metrics().encoded_column_cache_hits.sum();
        assert_eq!(
            accelerator.select_membership(MembershipSelection {
                left: &left,
                right: &right,
                keep_matches: true,
            }),
            AccelerationOutcome::Selected(vec![1, 3])
        );
        assert_eq!(
            accelerator.select_membership(MembershipSelection {
                left: &left,
                right: &right,
                keep_matches: false,
            }),
            AccelerationOutcome::Selected(vec![0, 2])
        );
        assert!(metrics().encoded_column_cache_hits.sum() >= cache_hits + 2);
        let string_left = Arc::from(
            ["gamma", "alpha", "missing", "alpha"]
                .map(Value::string)
                .to_vec(),
        );
        let string_right = Arc::from(["alpha", "beta"].map(Value::string).to_vec());
        assert_eq!(
            accelerator.select_membership(MembershipSelection {
                left: &string_left,
                right: &string_right,
                keep_matches: true,
            }),
            AccelerationOutcome::Selected(vec![1, 3])
        );
        assert_eq!(
            accelerator
                .output_pool
                .lock()
                .unwrap()
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            1
        );
    }
}
