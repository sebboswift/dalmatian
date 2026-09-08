{{- define "dalmatian.name" -}}
dalmatian
{{- end }}

{{- define "dalmatian.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "dalmatian.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "dalmatian.labels" -}}
app.kubernetes.io/name: {{ include "dalmatian.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "dalmatian.dataClaim" -}}
{{- if .Values.data.existingClaim -}}
{{ .Values.data.existingClaim }}
{{- else -}}
{{ include "dalmatian.fullname" . }}-data
{{- end -}}
{{- end }}

{{- define "dalmatian.outputClaim" -}}
{{- if .Values.outputs.existingClaim -}}
{{ .Values.outputs.existingClaim }}
{{- else -}}
{{ include "dalmatian.fullname" . }}-outputs
{{- end -}}
{{- end }}

{{- define "dalmatian.valkeyClaim" -}}
{{- if .Values.valkey.persistence.existingClaim -}}
{{ .Values.valkey.persistence.existingClaim }}
{{- else -}}
{{ include "dalmatian.fullname" . }}-valkey
{{- end -}}
{{- end }}

{{- define "dalmatian.sparkMaster" -}}
{{- if .Values.config.sparkMaster -}}
{{ .Values.config.sparkMaster }}
{{- else if .Values.spark.enabled -}}
spark://{{ include "dalmatian.fullname" . }}-spark-master:7077
{{- else -}}
{{ required "config.sparkMaster is required when spark.enabled=false" .Values.config.sparkMaster }}
{{- end -}}
{{- end }}

{{- define "dalmatian.valkeyUrl" -}}
{{- if .Values.config.valkeyUrl -}}
{{ .Values.config.valkeyUrl }}
{{- else if .Values.valkey.enabled -}}
valkey://{{ include "dalmatian.fullname" . }}-valkey:6379/0
{{- else -}}
{{ required "config.valkeyUrl is required when valkey.enabled=false" .Values.config.valkeyUrl }}
{{- end -}}
{{- end }}

{{- define "dalmatian.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ default (include "dalmatian.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end }}

{{- define "dalmatian.commonEnv" -}}
- name: DALMATIAN_SPARK_MASTER
  value: {{ include "dalmatian.sparkMaster" . | quote }}
- name: DALMATIAN_VALKEY_URL
  value: {{ include "dalmatian.valkeyUrl" . | quote }}
- name: DALMATIAN_SPARK_APP_MAX_CORES
  value: {{ .Values.config.sparkAppMaxCores | quote }}
- name: DALMATIAN_SPARK_EXECUTOR_CORES
  value: {{ .Values.config.sparkExecutorCores | quote }}
- name: DALMATIAN_SPARK_EXECUTOR_MEMORY
  value: {{ .Values.config.sparkExecutorMemory | quote }}
{{- if .Values.config.sparkExecutorInstances }}
- name: DALMATIAN_SPARK_EXECUTOR_INSTANCES
  value: {{ .Values.config.sparkExecutorInstances | quote }}
{{- end }}
- name: DALMATIAN_DATA_ROOT
  value: {{ .Values.config.dataRoot | quote }}
- name: DALMATIAN_SOURCE_LOCATIONS
  value: {{ .Values.config.sourceLocations | toJson | quote }}
- name: DALMATIAN_STORAGE_LOCATIONS
  value: {{ .Values.config.storageLocations | toJson | quote }}
- name: DALMATIAN_OUTPUT_PUBLICATION_PREFIX
  value: {{ .Values.config.outputPublicationPrefix | quote }}
{{- if .Values.config.metadataUrlSecret.name }}
- name: DALMATIAN_METADATA_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.config.metadataUrlSecret.name | quote }}
      key: {{ .Values.config.metadataUrlSecret.key | quote }}
{{- else if .Values.config.metadataUrl }}
- name: DALMATIAN_METADATA_URL
  value: {{ .Values.config.metadataUrl | quote }}
{{- end }}
- name: DALMATIAN_SPARK_PACKAGES
  value: {{ .Values.config.sparkPackages | quote }}
- name: DALMATIAN_SPARK_CONFIG
  value: {{ .Values.config.sparkConfig | toJson | quote }}
- name: DALMATIAN_DEFAULT_CACHE_TTL_SECONDS
  value: {{ .Values.config.defaultCacheTtlSeconds | quote }}
- name: DALMATIAN_MAX_CACHE_TTL_SECONDS
  value: {{ .Values.config.maxCacheTtlSeconds | quote }}
- name: DALMATIAN_RESULT_CACHE_MAX_BYTES
  value: {{ .Values.config.resultCacheMaxBytes | quote }}
- name: DALMATIAN_RESULT_CACHE_SINGLEFLIGHT_WAIT_SECONDS
  value: {{ .Values.config.resultCacheSingleflightWaitSeconds | quote }}
- name: DALMATIAN_RESULT_CACHE_LOCK_SECONDS
  value: {{ .Values.config.resultCacheLockSeconds | quote }}
- name: DALMATIAN_RESULT_ROW_LIMIT
  value: {{ .Values.config.resultRowLimit | quote }}
- name: DALMATIAN_MAX_INLINE_BYTES
  value: {{ .Values.config.maxInlineBytes | quote }}
- name: DALMATIAN_DATASET_CACHE_ENTRIES
  value: {{ .Values.config.datasetCacheEntries | quote }}
- name: DALMATIAN_DATASET_CACHE_TTL_SECONDS
  value: {{ .Values.config.datasetCacheTtlSeconds | quote }}
- name: DALMATIAN_MAX_CONCURRENT_QUERIES
  value: {{ .Values.config.maxConcurrentQueries | quote }}
- name: DALMATIAN_ADMISSION_WAIT_SECONDS
  value: {{ .Values.config.admissionWaitSeconds | quote }}
- name: DALMATIAN_DEFAULT_QUERY_TIMEOUT_SECONDS
  value: {{ .Values.config.defaultQueryTimeoutSeconds | quote }}
- name: DALMATIAN_MAX_QUERY_TIMEOUT_SECONDS
  value: {{ .Values.config.maxQueryTimeoutSeconds | quote }}
- name: DALMATIAN_JOB_TTL_SECONDS
  value: {{ .Values.config.jobTtlSeconds | quote }}
- name: DALMATIAN_IDEMPOTENCY_TTL_SECONDS
  value: {{ .Values.config.idempotencyTtlSeconds | quote }}
- name: DALMATIAN_JOB_LEASE_SECONDS
  value: {{ .Values.config.jobLeaseSeconds | quote }}
- name: DALMATIAN_WORKER_RESERVE_TIMEOUT_SECONDS
  value: {{ .Values.config.workerReserveTimeoutSeconds | quote }}
- name: DALMATIAN_WORKER_REAP_INTERVAL_SECONDS
  value: {{ .Values.config.workerReapIntervalSeconds | quote }}
- name: DALMATIAN_WORKER_MAX_ATTEMPTS
  value: {{ .Values.config.workerMaxAttempts | quote }}
- name: DALMATIAN_WORKER_RETRY_BASE_SECONDS
  value: {{ .Values.config.workerRetryBaseSeconds | quote }}
- name: DALMATIAN_WORKER_RETRY_MAX_SECONDS
  value: {{ .Values.config.workerRetryMaxSeconds | quote }}
- name: DALMATIAN_WORKER_AFFINITY_SHARDS
  value: {{ .Values.config.workerAffinityShards | quote }}
- name: DALMATIAN_MAX_QUEUE_DEPTH
  value: {{ .Values.config.maxQueueDepth | quote }}
- name: DALMATIAN_WORKER_METRICS_HOST
  value: "0.0.0.0"
- name: DALMATIAN_WORKER_METRICS_PORT
  value: {{ .Values.monitoring.workerMetricsPort | quote }}
- name: DALMATIAN_LOG_LEVEL
  value: {{ .Values.config.logLevel | quote }}
{{- end }}
