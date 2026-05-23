type DatasetToolbarProps = {
  canManage: boolean;
  onAddEmployee: () => void;
  onApprove: () => void;
  onBulk: () => void;
  onEdit: () => void;
  onExport: () => void;
  onReports: () => void;
  onUpload: () => void;
};

export function DatasetToolbar({
  canManage,
  onAddEmployee,
  onApprove,
  onBulk,
  onEdit,
  onExport,
  onReports,
  onUpload
}: DatasetToolbarProps) {
  return (
    <section className="hr-modern-toolbar" aria-label="HR dataset actions">
      <button type="button" onClick={onAddEmployee} disabled={!canManage}>+ Add Employee</button>
      <button type="button" onClick={onUpload}>Upload</button>
      <button type="button" onClick={onEdit}>Edit</button>
      <button type="button" onClick={onApprove} disabled={!canManage}>Approve</button>
      <button type="button" onClick={onReports}>Reports</button>
      <button type="button" onClick={onExport}>Export</button>
      <button type="button" onClick={onBulk} disabled={!canManage}>Bulk Actions</button>
    </section>
  );
}
