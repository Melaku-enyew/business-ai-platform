import { ReactNode } from 'react';

type EmployeeDrawerProps = {
  children: ReactNode;
  onClose: () => void;
  title: string;
};

export function EmployeeDrawer({ children, onClose, title }: EmployeeDrawerProps) {
  return (
    <div className="modal-backdrop" role="presentation">
      <aside className="employee-workspace-modal employee-drawer-shell" aria-label={title}>
        <div className="panel-header">
          <h2>{title}</h2>
          <button className="ghost-button compact" type="button" onClick={onClose}>Close</button>
        </div>
        {children}
      </aside>
    </div>
  );
}
