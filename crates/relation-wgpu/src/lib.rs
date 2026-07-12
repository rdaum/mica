// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

//! Vulkan-backed execution for selected packed relation operators.

use mica_relation_kernel::{
    AccelerationDecline, AccelerationOutcome, MembershipSelection, RelationAccelerator,
};
use mica_var::{Identity, Value};
use std::fmt;
use std::sync::mpsc;
use wgpu::util::DeviceExt;

const WORKGROUP_SIZE: u32 = 256;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BufferMode {
    StagedReadback,
    SharedMappable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueEncoding {
    Int,
    Identity,
    Float,
}

pub struct WgpuAccelerator {
    adapter_name: String,
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    buffer_mode: BufferMode,
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
        })
    }

    pub fn adapter_name(&self) -> &str {
        &self.adapter_name
    }

    pub fn uses_shared_mappable_buffers(&self) -> bool {
        self.buffer_mode == BufferMode::SharedMappable
    }

    fn execute_membership(
        &self,
        left: &[u64],
        right: &[u64],
        keep_matches: bool,
    ) -> Result<Vec<usize>, String> {
        if left.is_empty() {
            return Ok(Vec::new());
        }
        if right.is_empty() {
            return Ok(if keep_matches {
                Vec::new()
            } else {
                (0..left.len()).collect()
            });
        }
        let input_usage = match self.buffer_mode {
            BufferMode::StagedReadback => wgpu::BufferUsages::STORAGE,
            BufferMode::SharedMappable => {
                wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::MAP_WRITE
            }
        };
        let left_buffer = create_u64_buffer(&self.device, "mica-relation-left", left, input_usage);
        let right_buffer =
            create_u64_buffer(&self.device, "mica-relation-right", right, input_usage);
        let output_size = (left.len() * size_of::<u32>()) as u64;
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
            size: output_size,
            usage: output_usage,
            mapped_at_creation: false,
        });
        let readback = (self.buffer_mode == BufferMode::StagedReadback).then(|| {
            self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("mica-relation-membership-readback"),
                size: output_size,
                usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            })
        });
        let params = [left.len() as u32, right.len() as u32, 0, 0];
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
                    resource: left_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: right_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: output.as_entire_binding(),
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
            pass.dispatch_workgroups((left.len() as u32).div_ceil(WORKGROUP_SIZE), 1, 1);
        }
        let output_readback = readback.as_ref().unwrap_or(&output);
        if let Some(readback) = &readback {
            encoder.copy_buffer_to_buffer(&output, 0, readback, 0, output_size);
        }
        self.queue.submit([encoder.finish()]);

        let output_slice = output_readback.slice(..);
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
        Ok(selected)
    }
}

impl RelationAccelerator for WgpuAccelerator {
    fn select_membership(&self, selection: MembershipSelection<'_>) -> AccelerationOutcome {
        let Some(encoding) = detect_encoding(selection.left, selection.right) else {
            return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedDomain);
        };
        let Some(left) = encode_column(selection.left, encoding) else {
            return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedDomain);
        };
        let Some(mut right) = encode_column(selection.right, encoding) else {
            return AccelerationOutcome::Declined(AccelerationDecline::UnsupportedDomain);
        };
        right.sort_unstable();
        right.dedup();
        match self.execute_membership(&left, &right, selection.keep_matches) {
            Ok(selected) => AccelerationOutcome::Selected(selected),
            Err(_) => AccelerationOutcome::Declined(AccelerationDecline::Failed),
        }
    }
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
    #[ignore = "requires a Vulkan adapter with shaderInt64"]
    fn hardware_membership_selects_original_row_indexes() {
        let accelerator = WgpuAccelerator::new(WgpuAcceleratorOptions::default()).unwrap();
        let left = Arc::from(
            [30, 10, 40, 10]
                .map(|value| Value::int(value).unwrap())
                .to_vec(),
        );
        let right = Arc::from([10, 20].map(|value| Value::int(value).unwrap()).to_vec());
        assert_eq!(
            accelerator.select_membership(MembershipSelection {
                left: &left,
                right: &right,
                keep_matches: true,
            }),
            AccelerationOutcome::Selected(vec![1, 3])
        );
    }
}
