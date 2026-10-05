export type DeviceMessageHandler = (message: unknown) => void

export interface DeviceSubscription {
  unsubscribe(): void
}

export function subscribe(_deviceKey: string, _handler: DeviceMessageHandler): DeviceSubscription {
  return { unsubscribe() {} }
}
