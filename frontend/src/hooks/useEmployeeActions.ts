export function useEmployeeActions() {
  function isSensitiveField(field: string) {
    return /salary|tax|benefit|payroll/i.test(field);
  }

  return { isSensitiveField };
}
