const FIELD_DISPLAY_SYNC_EVENT = 'field-display-config-changed'
const FIELD_DISPLAY_SYNC_STORAGE_KEY = 'field-display-config-changed'

type FieldDisplaySyncDetail = {
  tableName: string
  timestamp: number
}

export function notifyFieldDisplayConfigChanged(tableName: string) {
  if (!tableName) {
    return
  }

  const detail: FieldDisplaySyncDetail = {
    tableName,
    timestamp: Date.now()
  }

  window.dispatchEvent(new CustomEvent<FieldDisplaySyncDetail>(FIELD_DISPLAY_SYNC_EVENT, { detail }))
  window.localStorage.setItem(FIELD_DISPLAY_SYNC_STORAGE_KEY, JSON.stringify(detail))
}

export function subscribeFieldDisplayConfigChange(handler: (tableName: string) => void) {
  const handleCustomEvent = (event: Event) => {
    const customEvent = event as CustomEvent<FieldDisplaySyncDetail>
    if (customEvent.detail?.tableName) {
      handler(customEvent.detail.tableName)
    }
  }

  const handleStorageEvent = (event: StorageEvent) => {
    if (event.key !== FIELD_DISPLAY_SYNC_STORAGE_KEY || !event.newValue) {
      return
    }

    try {
      const detail = JSON.parse(event.newValue) as FieldDisplaySyncDetail
      if (detail.tableName) {
        handler(detail.tableName)
      }
    } catch {
      // Ignore malformed sync payloads.
    }
  }

  window.addEventListener(FIELD_DISPLAY_SYNC_EVENT, handleCustomEvent as EventListener)
  window.addEventListener('storage', handleStorageEvent)

  return () => {
    window.removeEventListener(FIELD_DISPLAY_SYNC_EVENT, handleCustomEvent as EventListener)
    window.removeEventListener('storage', handleStorageEvent)
  }
}
