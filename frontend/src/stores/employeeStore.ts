export type EmployeeStoreSnapshot = {
  activeEmployeeId: string;
  drawerOpen: boolean;
};

export const employeeStoreDefaults: EmployeeStoreSnapshot = {
  activeEmployeeId: '',
  drawerOpen: false
};
