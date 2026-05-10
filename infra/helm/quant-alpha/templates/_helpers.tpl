{{/*
Expand the name of the chart.
*/}}
{{- define "quant-alpha.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully-qualified app name (chart + release).
*/}}
{{- define "quant-alpha.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Chart label: chart name + version.
*/}}
{{- define "quant-alpha.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "quant-alpha.labels" -}}
helm.sh/chart: {{ include "quant-alpha.chart" . }}
{{ include "quant-alpha.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/environment: {{ .Values.global.environment }}
{{- end }}

{{/*
Selector labels (stable across upgrades — do not change).
*/}}
{{- define "quant-alpha.selectorLabels" -}}
app.kubernetes.io/name: {{ include "quant-alpha.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "quant-alpha.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "quant-alpha.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Full image reference: repository:tag.
*/}}
{{- define "quant-alpha.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Secret name — either the managed secret or the external one.
*/}}
{{- define "quant-alpha.secretName" -}}
{{ .Values.secrets.secretName }}
{{- end }}

{{/*
Volume + volumeMount block for the data PVC (used in every workload).
*/}}
{{- define "quant-alpha.dataVolume" -}}
- name: quant-alpha-data
  persistentVolumeClaim:
    claimName: {{ .Values.persistence.claimName }}
{{- end }}

{{- define "quant-alpha.dataVolumeMount" -}}
- name: quant-alpha-data
  mountPath: /app/data
{{- end }}
