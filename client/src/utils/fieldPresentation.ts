export interface FieldDisplaySettings {
  formatAsWanYi?: boolean
  formatAsTimestamp?: boolean
  valueMapping?: Record<string, string>
  unit?: string
  textClamp?: boolean
}

const TIMESTAMP_FIELDS = new Set(['pubDate', 'updateDate', 'attachedPubDate'])

export function formatAsWanYi(value: number): string {
  const absValue = Math.abs(value)
  const sign = value < 0 ? '-' : ''

  if (absValue >= 100000000) {
    return sign + (absValue / 100000000).toFixed(2) + '亿'
  }

  if (absValue >= 10000) {
    return sign + (absValue / 10000).toFixed(2) + '万'
  }

  return value.toFixed(2)
}

export function formatTimestamp(value: number): string {
  const timestamp = value > 9999999999 ? value : value * 1000
  const date = new Date(timestamp)

  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'Asia/Shanghai'
  })
}

export function formatDisplayValue(
  value: unknown,
  type: string,
  fieldName?: string,
  settings: FieldDisplaySettings = {}
): string {
  if (value === null || value === undefined) {
    return '-'
  }

  if (fieldName === 'ggeid' && value) {
    const numValue = typeof value === 'number' ? value : parseFloat(String(value))
    if (!Number.isNaN(numValue)) {
      return numValue.toFixed(0)
    }
  }

  const unit = settings.unit || ''
  const valueMapping = settings.valueMapping || {}
  const valueKey = String(value)
  if (valueKey in valueMapping && valueMapping[valueKey]) {
    return valueMapping[valueKey]
  }

  if (settings.formatAsTimestamp && fieldName) {
    const numValue = typeof value === 'number' ? value : parseInt(String(value), 10)
    if (!Number.isNaN(numValue) && numValue > 946684800) {
      return formatTimestamp(numValue)
    }
  }

  if (fieldName && TIMESTAMP_FIELDS.has(fieldName)) {
    const timestamp = typeof value === 'string' ? parseInt(value, 10) : Number(value)
    if (!Number.isNaN(timestamp) && timestamp > 1000000000000) {
      return new Date(timestamp).toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      })
    }
  }

  if (settings.formatAsWanYi) {
    const numValue = typeof value === 'number' ? value : parseFloat(String(value))
    if (!Number.isNaN(numValue)) {
      const formatted = formatAsWanYi(numValue)
      return unit ? `${formatted}${unit}` : formatted
    }
  }

  if (unit) {
    if (typeof value === 'number') {
      if (type === 'REAL' || !Number.isInteger(value)) {
        return `${value.toFixed(2)}${unit}`
      }

      return `${value}${unit}`
    }

    const numValue = parseFloat(String(value))
    if (!Number.isNaN(numValue)) {
      return `${value}${unit}`
    }
  }

  if (type === 'REAL' && typeof value === 'number') {
    return value.toFixed(2)
  }

  if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
    try {
      return JSON.stringify(JSON.parse(value), null, 2)
    } catch {
      return value
    }
  }

  return String(value)
}

export function getDisplayValueClass(value: unknown, type: string): string {
  if (value === null || value === undefined) {
    return 'value-null'
  }

  if (type === 'INTEGER' || type === 'REAL') {
    return 'value-number'
  }

  return ''
}

export function shouldClampDisplayCell(value: unknown, settings: FieldDisplaySettings = {}): boolean {
  if (!settings.textClamp) {
    return false
  }

  if (value === null || value === undefined) {
    return false
  }

  return String(value).length > 0
}
