//! Boundary between YAML sugar and [`EngineConfig`].
//!
//! Invalid field paths and aggregation kinds become [`AggError::Config`] here, and the core
//! crate never parses the untagged aggregation DSL.

use logsluice_core::config::{
    AggregationKind, AggregationSpec, EngineConfig, StorageConfig as CoreStorageConfig,
    WindowConfig,
};
use logsluice_core::error::{AggError, AggResult};

use crate::config::{AggregationDefinition, NormalizerConfig, SluiceConfig};
use crate::event::FieldPath;

impl SluiceConfig {
    pub(super) fn compile_engine(&self) -> AggResult<EngineConfig> {
        let _ = FieldPath::parse(&self.window.timestamp_field).map_err(AggError::Config)?;
        let _ = FieldPath::parse(&self.window.source_id_field).map_err(AggError::Config)?;
        if !self.output.route_field.is_empty() {
            let _ = FieldPath::parse(&self.output.route_field).map_err(AggError::Config)?;
        }
        if !self.attributes_policy.template_target_field.is_empty() {
            let _ = FieldPath::parse(&self.attributes_policy.template_target_field)
                .map_err(AggError::Config)?;
        }
        if let Some(ref sp) = self.source_position {
            let _ = FieldPath::parse(&sp.topic_field).map_err(AggError::Config)?;
            let _ = FieldPath::parse(&sp.partition_field).map_err(AggError::Config)?;
            let _ = FieldPath::parse(&sp.offset_field).map_err(AggError::Config)?;
        }
        for dim in &self.group_by {
            let _ = FieldPath::parse(dim).map_err(AggError::Config)?;
        }
        for norm in self.normalizers.values() {
            if let NormalizerConfig::IpCidr {
                when: Some(cond), ..
            } = norm
            {
                let _ = FieldPath::parse(&cond.field).map_err(AggError::Config)?;
            }
        }

        let mut specs = Vec::with_capacity(self.aggregations.len());

        for (name, def) in &self.aggregations {
            let _ = FieldPath::parse(name).map_err(AggError::Config)?;
            match def {
                AggregationDefinition::Simple(kind_str) => match kind_str.as_str() {
                    "count" => specs.push(AggregationSpec {
                        name: name.clone(),
                        kind: AggregationKind::Count,
                        field: None,
                    }),
                    other => {
                        return Err(AggError::Config(format!(
                            "unsupported aggregation kind '{other}' for metric '{name}'"
                        )));
                    }
                },
                AggregationDefinition::Detailed(map) => {
                    if map.is_empty() {
                        return Err(AggError::Config(format!(
                            "empty aggregation definition for metric '{name}'"
                        )));
                    }

                    let mut found = false;
                    for (kind_str, field_str) in map {
                        let _ = FieldPath::parse(field_str).map_err(AggError::Config)?;
                        match kind_str.as_str() {
                            "count" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Count,
                                    field: None,
                                });
                                found = true;
                            }
                            "sum" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Sum,
                                    field: Some(field_str.clone()),
                                });
                                found = true;
                            }
                            "min" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Min,
                                    field: Some(field_str.clone()),
                                });
                                found = true;
                            }
                            "max" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Max,
                                    field: Some(field_str.clone()),
                                });
                                found = true;
                            }
                            other => {
                                return Err(AggError::Config(format!(
                                    "unsupported aggregation kind '{other}' for metric '{name}'"
                                )));
                            }
                        }
                    }

                    if !found {
                        return Err(AggError::Config(format!(
                            "no valid aggregation kind found for metric '{name}'"
                        )));
                    }
                }
            }
        }

        let storage = CoreStorageConfig {
            data_dir: self.storage.data_dir.clone(),
            cache_size_total: self.storage.cache_size_total,
            l1_coalesce_max_keys: self.storage.l1_coalesce_max_keys,
            l1_flush_interval: self.storage.l1_flush_interval,
            hdr_max_memtable_size: self.storage.hdr_max_memtable_size,
            tail_max_memtable_size: self.storage.tail_max_memtable_size,
            max_disk_bytes: self.storage.max_disk_bytes,
            control_reserve_bytes: self.storage.control_reserve_bytes,
            on_disk_full: self.storage.on_disk_full,
            sync_wal: self.storage.sync_wal,
            worker_threads: self.storage.worker_threads,
            max_write_buffer_bytes: self.storage.max_write_buffer_bytes,
            max_cached_files: self.storage.max_cached_files,
        };

        let window = WindowConfig {
            duration: self.window.duration,
            offset: self.window.offset,
            timestamp_field: self.window.timestamp_field.clone(),
            allowed_skew: self.window.allowed_skew,
            allowed_lateness: self.window.allowed_lateness,
            max_future_drift: self.window.max_future_drift,
            idle_timeout: self.window.idle_timeout,
            caught_up_threshold: self.window.caught_up_threshold,
            max_close_delay: self.window.max_close_delay,
            late_emit_interval: self.window.late_emit_interval,
            max_closing_duration: self.window.max_closing_duration,
            source_id_field: self.window.source_id_field.clone(),
            max_tracked_sources: self.window.max_tracked_sources,
            emit: self.window.emit.clone().into(),
        };

        let engine = EngineConfig {
            shards: self.shards,
            emit_mode: self.emit_mode,
            storage,
            window,
            cardinality: self.cardinality.clone().into(),
            output: self.output.clone().into(),
            shutdown: self.shutdown.clone().into(),
            aggregations: specs,
        };

        engine.validate()?;
        Ok(engine)
    }
}
