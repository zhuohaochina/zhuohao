import type { RelativeDateRule } from '../models/types';

export interface DateScopeOptions {
  dateRange?: number;
  dateRangeMode?: 'offset_days' | 'count_including_today';
  dateUnit?: 'calendar_day' | 'trading_day';
  closedDateRanges?: Array<{ begin: string; end: string }>;
  futureDays?: number;
  fixedBeginDate?: string | null;
  relativeDateRule?: RelativeDateRule | null;
  referenceDate?: Date;
}

export interface DateScopeResult {
  startDate: Date;
  endDate: Date;
  noDateFilter: boolean;
}

const BEIJING_OFFSET_MS = 8 * 60 * 60 * 1000;

function cloneDate(date: Date): Date {
  return new Date(date.getTime());
}

function startOfBeijingDate(referenceDate: Date = new Date()): Date {
  const beijingDate = new Date(referenceDate.getTime() + BEIJING_OFFSET_MS);
  beijingDate.setUTCHours(0, 0, 0, 0);
  return beijingDate;
}

function addDays(date: Date, days: number): Date {
  const next = cloneDate(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function dateKey(date: Date): string {
  return formatDateOnly(date);
}

function isClosedDate(date: Date, closedDateRanges: Array<{ begin: string; end: string }> = []): boolean {
  const key = dateKey(date);
  return closedDateRanges.some(range => key >= range.begin && key <= range.end);
}

function isTradingDate(date: Date, closedDateRanges: Array<{ begin: string; end: string }> = []): boolean {
  const day = date.getUTCDay();
  return day >= 1 && day <= 5 && !isClosedDate(date, closedDateRanges);
}

function addTradingDays(date: Date, days: number, closedDateRanges: Array<{ begin: string; end: string }> = []): Date {
  if (days === 0) {
    return cloneDate(date);
  }

  const direction = days > 0 ? 1 : -1;
  let remaining = Math.abs(days);
  const next = cloneDate(date);

  while (remaining > 0) {
    next.setUTCDate(next.getUTCDate() + direction);
    if (isTradingDate(next, closedDateRanges)) {
      remaining--;
    }
  }

  return next;
}

function resolveTradingEndDate(today: Date, futureDays = 0, closedDateRanges: Array<{ begin: string; end: string }> = []): Date {
  if (futureDays > 0) {
    return addTradingDays(today, futureDays, closedDateRanges);
  }

  let endDate = cloneDate(today);
  while (!isTradingDate(endDate, closedDateRanges)) {
    endDate = addDays(endDate, -1);
  }
  return endDate;
}

function mod(value: number, divisor: number): number {
  return ((value % divisor) + divisor) % divisor;
}

function resolveRelativeWeekday(rule: RelativeDateRule, referenceDate: Date = new Date()): Date {
  const weekday = mod(rule.weekday, 7);
  const weekStartsOn = mod(rule.weekStartsOn ?? 1, 7);
  const weeksAgo = Math.max(1, Math.trunc(rule.weeksAgo ?? 1));
  const today = startOfBeijingDate(referenceDate);
  const currentWeekOffset = mod(today.getUTCDay() - weekStartsOn, 7);
  const currentWeekStart = addDays(today, -currentWeekOffset);
  const targetWeekStart = addDays(currentWeekStart, -7 * weeksAgo);
  const targetOffset = mod(weekday - weekStartsOn, 7);

  return addDays(targetWeekStart, targetOffset);
}

export function formatDateOnly(date: Date): string {
  return cloneDate(date).toISOString().split('T')[0];
}

export function calculateDateScope(options?: DateScopeOptions): DateScopeResult {
  const today = startOfBeijingDate(options?.referenceDate);
  const isTradingDayScope = options?.dateUnit === 'trading_day';
  const closedDateRanges = options?.closedDateRanges || [];

  if (options?.relativeDateRule?.mode === 'relative_weekday') {
    const targetDate = resolveRelativeWeekday(options.relativeDateRule, options.referenceDate);
    return {
      startDate: targetDate,
      endDate: targetDate,
      noDateFilter: false
    };
  }

  let startDate: Date;
  let noDateFilter = false;

  if (options?.fixedBeginDate) {
    startDate = new Date(options.fixedBeginDate);
  } else if (options?.dateRange === -1) {
    startDate = new Date('2000-01-01');
    noDateFilter = true;
  } else if (typeof options?.dateRange === 'number' && options.dateRange > 0) {
    const backwardDays = options.dateRangeMode === 'count_including_today'
      ? Math.max(0, options.dateRange - 1)
      : options.dateRange;
    if (isTradingDayScope) {
      const tradingEndDate = resolveTradingEndDate(today, options?.futureDays || 0, closedDateRanges);
      startDate = addTradingDays(tradingEndDate, -backwardDays, closedDateRanges);
      return { startDate, endDate: tradingEndDate, noDateFilter };
    }
    startDate = addDays(today, -backwardDays);
  } else {
    startDate = new Date('2000-01-01');
    noDateFilter = true;
  }

  const endDate = isTradingDayScope
    ? resolveTradingEndDate(today, options?.futureDays || 0, closedDateRanges)
    : addDays(today, options?.futureDays || 0);

  return { startDate, endDate, noDateFilter };
}

export function hasDateScope(options?: DateScopeOptions): boolean {
  return Boolean(
    options?.fixedBeginDate ||
    options?.relativeDateRule ||
    options?.dateRange === -1 ||
    (typeof options?.dateRange === 'number' && options.dateRange > 0)
  );
}
