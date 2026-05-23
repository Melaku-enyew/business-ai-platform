export type HrWorkspaceTab = {
  key: string;
  label: string;
  path: string;
};

type HRWorkspaceFrameProps = {
  activeKey: string;
  companyName: string;
  onAi: () => void;
  onNavigate: (path: string) => void;
  tabs: HrWorkspaceTab[];
};

export function HRWorkspaceFrame({ activeKey, companyName, onAi, onNavigate, tabs }: HRWorkspaceFrameProps) {
  const active = tabs.find((tab) => tab.key === activeKey);
  return (
    <>
      <div className="hr-workspace-shell-header">
        <div>
          <p className="eyebrow">HR & Workforce</p>
          <h2>{active?.label ?? 'Overview'} Workspace</h2>
          <span>{companyName} - one active dataset, focused tabs, inline editing, approvals, and reports.</span>
        </div>
        <button className="ghost-button compact" type="button" onClick={onAi}>Open AI Insights</button>
      </div>
      <nav className="hr-workspace-nav" aria-label="HR workspaces">
        {tabs.map((tab) => (
          <button className={activeKey === tab.key ? 'active' : ''} key={tab.key} type="button" onClick={() => onNavigate(tab.path)}>
            {tab.label}
          </button>
        ))}
      </nav>
    </>
  );
}
