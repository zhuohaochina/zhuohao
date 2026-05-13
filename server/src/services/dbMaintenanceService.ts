import fs from 'fs';
import path from 'path';
import { execNative, getDatabase, getDatabasePath } from './database';

interface DbSpaceStats {
  dbPath: string;
  walPath: string;
  pageSize: number;
  pageCount: number;
  freelistCount: number;
  usedPageCount: number;
  totalBytes: number;
  freeBytes: number;
  usedBytes: number;
  totalMb: number;
  freeMb: number;
  usedMb: number;
  freeRatio: number;
  walBytes: number;
  walMb: number;
}

interface DbMaintenanceState {
  lastCompactAt?: string;
  lastTrigger?: string;
  lastDurationMs?: number;
  lastBefore?: DbSpaceStats;
  lastAfter?: DbSpaceStats;
}

interface CompactDecision {
  shouldCompact: boolean;
  reason: string;
}

function readBooleanEnv(name: string, fallback: boolean): boolean {
  const value = process.env[name];
  if (value === undefined) return fallback;
  return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
}

function readNumberEnv(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

const AUTO_MAINTENANCE_ENABLED = readBooleanEnv('DB_AUTO_MAINTENANCE_ENABLED', true);
const COMPACT_FREE_RATIO_THRESHOLD = readNumberEnv('DB_AUTO_COMPACT_FREE_RATIO_THRESHOLD', 0.35);
const COMPACT_MIN_FREE_MB = readNumberEnv('DB_AUTO_COMPACT_MIN_FREE_MB', 256);
const COMPACT_MIN_DB_MB = readNumberEnv('DB_AUTO_COMPACT_MIN_DB_MB', 512);
const COMPACT_MIN_INTERVAL_HOURS = readNumberEnv('DB_AUTO_COMPACT_MIN_INTERVAL_HOURS', 24);
const WAL_TRUNCATE_THRESHOLD_MB = readNumberEnv('DB_AUTO_WAL_TRUNCATE_THRESHOLD_MB', 64);

const MB = 1024 * 1024;

class DbMaintenanceService {
  private running = false;

  private getStateFilePath(): string {
    const dbPath = getDatabasePath();
    return path.join(path.dirname(dbPath), 'db-maintenance-state.json');
  }

  private loadState(): DbMaintenanceState {
    const statePath = this.getStateFilePath();
    if (!fs.existsSync(statePath)) {
      return {};
    }

    try {
      const raw = fs.readFileSync(statePath, 'utf8');
      const parsed = JSON.parse(raw);
      return typeof parsed === 'object' && parsed ? parsed as DbMaintenanceState : {};
    } catch {
      return {};
    }
  }

  private saveState(state: DbMaintenanceState): void {
    try {
      fs.writeFileSync(this.getStateFilePath(), JSON.stringify(state, null, 2), 'utf8');
    } catch (error) {
      console.warn('[DBMaintenance] 保存维护状态失败:', (error as Error).message);
    }
  }

  private collectStats(): DbSpaceStats {
    const db = getDatabase();
    const dbPath = getDatabasePath();
    const walPath = `${dbPath}-wal`;

    const pageSize = Number(db.pragma('page_size', { simple: true }) || 0) || 4096;
    const pageCount = Number(db.pragma('page_count', { simple: true }) || 0);
    const freelistCount = Number(db.pragma('freelist_count', { simple: true }) || 0);
    const usedPageCount = Math.max(pageCount - freelistCount, 0);

    const totalBytes = pageSize * pageCount;
    const freeBytes = pageSize * freelistCount;
    const usedBytes = pageSize * usedPageCount;
    const freeRatio = pageCount > 0 ? freelistCount / pageCount : 0;

    const walBytes = fs.existsSync(walPath) ? fs.statSync(walPath).size : 0;

    return {
      dbPath,
      walPath,
      pageSize,
      pageCount,
      freelistCount,
      usedPageCount,
      totalBytes,
      freeBytes,
      usedBytes,
      totalMb: totalBytes / MB,
      freeMb: freeBytes / MB,
      usedMb: usedBytes / MB,
      freeRatio,
      walBytes,
      walMb: walBytes / MB
    };
  }

  private shouldCompact(stats: DbSpaceStats, state: DbMaintenanceState): CompactDecision {
    if (stats.totalMb < COMPACT_MIN_DB_MB) {
      return { shouldCompact: false, reason: `db_too_small(${stats.totalMb.toFixed(1)}MB)` };
    }

    if (stats.freeMb < COMPACT_MIN_FREE_MB) {
      return { shouldCompact: false, reason: `free_mb_below_threshold(${stats.freeMb.toFixed(1)}MB)` };
    }

    if (stats.freeRatio < COMPACT_FREE_RATIO_THRESHOLD) {
      return { shouldCompact: false, reason: `free_ratio_below_threshold(${(stats.freeRatio * 100).toFixed(2)}%)` };
    }

    if (state.lastCompactAt) {
      const lastTs = Date.parse(state.lastCompactAt);
      const minIntervalMs = COMPACT_MIN_INTERVAL_HOURS * 60 * 60 * 1000;
      if (Number.isFinite(lastTs) && Date.now() - lastTs < minIntervalMs) {
        return { shouldCompact: false, reason: 'min_interval_not_reached' };
      }
    }

    return { shouldCompact: true, reason: 'threshold_matched' };
  }

  private maybeCheckpointWal(stats: DbSpaceStats, trigger: string): void {
    if (stats.walMb < WAL_TRUNCATE_THRESHOLD_MB) {
      return;
    }

    try {
      execNative('PRAGMA wal_checkpoint(TRUNCATE);');
      console.log(
        `[DBMaintenance] WAL checkpoint(TRUNCATE) completed [trigger=${trigger}] wal=${stats.walMb.toFixed(1)}MB`
      );
    } catch (error) {
      console.warn(`[DBMaintenance] WAL checkpoint 失败 [trigger=${trigger}]:`, (error as Error).message);
    }
  }

  async maybeAutoCompact(trigger: string): Promise<void> {
    if (!AUTO_MAINTENANCE_ENABLED) {
      return;
    }

    if (this.running) {
      return;
    }

    this.running = true;

    try {
      const before = this.collectStats();
      this.maybeCheckpointWal(before, trigger);

      const state = this.loadState();
      const decision = this.shouldCompact(before, state);
      if (!decision.shouldCompact) {
        return;
      }

      const startedAt = Date.now();
      console.log(
        `[DBMaintenance] Auto compact start [trigger=${trigger}] total=${before.totalMb.toFixed(1)}MB free=${before.freeMb.toFixed(1)}MB (${(before.freeRatio * 100).toFixed(2)}%)`
      );

      execNative('PRAGMA wal_checkpoint(TRUNCATE);');
      execNative('VACUUM;');
      execNative('PRAGMA wal_checkpoint(TRUNCATE);');

      const after = this.collectStats();
      const durationMs = Date.now() - startedAt;

      this.saveState({
        lastCompactAt: new Date().toISOString(),
        lastTrigger: trigger,
        lastDurationMs: durationMs,
        lastBefore: before,
        lastAfter: after
      });

      console.log(
        `[DBMaintenance] Auto compact done [trigger=${trigger}] duration=${durationMs}ms db ${before.totalMb.toFixed(1)}MB -> ${after.totalMb.toFixed(1)}MB`
      );
    } catch (error) {
      console.error(`[DBMaintenance] Auto compact failed [trigger=${trigger}]`, error);
    } finally {
      this.running = false;
    }
  }
}

export const dbMaintenanceService = new DbMaintenanceService();
