export type TimesheetStoreSnapshot = {
  activePayPeriod: string;
  activeTab: string;
};

export const timesheetStoreDefaults: TimesheetStoreSnapshot = {
  activePayPeriod: '',
  activeTab: 'Dashboard'
};
