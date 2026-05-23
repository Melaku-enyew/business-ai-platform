export function useTimesheets<T extends { metadata?: Record<string, unknown>; status?: string }>(records: T[]) {
  const active = records.filter((record) => record.status !== 'archived');
  const pending = active.filter((record) => String(record.metadata?.approvalStatus ?? record.status ?? '').includes('pending'));
  const overtime = active.filter((record) => Number(record.metadata?.overtimeHours ?? 0) > 0);
  return { active, overtime, pending };
}
