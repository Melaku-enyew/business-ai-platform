type TimesheetWorkspaceSummaryProps = {
  clockedIn: number;
  dailyEntries: number;
  missingPunches: number;
  overtime: number;
  pendingApproval: number;
};

export function TimesheetWorkspaceSummary({
  clockedIn,
  dailyEntries,
  missingPunches,
  overtime,
  pendingApproval
}: TimesheetWorkspaceSummaryProps) {
  return (
    <div className="hr-dashboard-grid compact-attendance-grid">
      <div><span>Clocked in</span><strong>{clockedIn}</strong></div>
      <div><span>Daily entries</span><strong>{dailyEntries}</strong></div>
      <div><span>Missing punches</span><strong>{missingPunches}</strong></div>
      <div><span>Over 40 hours</span><strong>{overtime}</strong></div>
      <div><span>Pending approval</span><strong>{pendingApproval}</strong></div>
    </div>
  );
}
