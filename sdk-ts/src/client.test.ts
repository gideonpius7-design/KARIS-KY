import {
  EscrowClient,
  type EscrowEvent,
  type SorobanEventQuery,
  type SorobanRpcClient,
} from "./index";

const event = (name: string, ledger: number): EscrowEvent => ({
  id: `${name}-${ledger}`,
  type: "contract",
  contract_id: "CESCROW",
  ledger,
  ledger_closed_at: "2026-01-01T00:00:00Z",
  paging_token: `${ledger}-token`,
  topics: [name],
  value: {},
  name,
});

test("streams pages, advances the cursor, and filters event names", async () => {
  const calls: Array<{ cursor?: string; startLedger?: number }> = [];
  const rpc: SorobanRpcClient = {
    invoke: jest.fn(),
    simulate: jest.fn(),
    getLedger: jest.fn().mockResolvedValue({ timestamp: 0, sequence: 42 }),
    getEvents: jest.fn().mockImplementation(async (_filter: unknown, options: SorobanEventQuery) => {
      calls.push(options);
      if (calls.length === 1) {
        return { events: [event("EscrowFunded", 42), event("EscrowSettled", 42)], cursor: "next" };
      }
      return { events: [event("EscrowSettled", 43)], latest_ledger: 43 };
    }),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);
  const controller = new AbortController();
  const stream = client.subscribeEscrowEvents({
    event_names: ["EscrowSettled"],
    poll_interval_ms: 0,
    signal: controller.signal,
  });

  expect((await stream.next()).value?.name).toBe("EscrowSettled");
  expect((await stream.next()).value?.name).toBe("EscrowSettled");
  controller.abort();
  expect(calls).toEqual([
    { startLedger: 42, limit: 100 },
    { cursor: "next", limit: 100 },
  ]);
});

test("fails clearly when getEvents is unavailable", async () => {
  const rpc: SorobanRpcClient = {
    invoke: jest.fn(),
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);
  await expect(client.subscribeEscrowEvents().next()).rejects.toThrow("does not support getEvents");
});

test("setLegalHold passes both active and reason arguments", async () => {
  const mockInvoke = jest.fn().mockResolvedValue(undefined);
  const rpc: SorobanRpcClient = {
    invoke: mockInvoke,
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);
  client.setContractId("CTEST123");

  // Test activating legal hold with reason
  await client.setLegalHold(true, "Compliance investigation", "source123");
  expect(mockInvoke).toHaveBeenCalledWith(
    "CTEST123",
    "set_legal_hold",
    [true, "Compliance investigation"],
    "source123"
  );

  // Test deactivating legal hold with empty reason
  mockInvoke.mockClear();
  await client.setLegalHold(false, "", "source456");
  expect(mockInvoke).toHaveBeenCalledWith(
    "CTEST123",
    "set_legal_hold",
    [false, ""],
    "source456"
  );
});

test("setLegalHold rejects empty reason when active=true", async () => {
  const rpc: SorobanRpcClient = {
    invoke: jest.fn(),
    simulate: jest.fn(),
    getLedger: jest.fn(),
  };
  const client = new EscrowClient({ rpcUrl: "http://localhost", networkPassphrase: "test" }, rpc);

  // Should throw when activating with empty reason
  await expect(client.setLegalHold(true, "")).rejects.toThrow(
    "reason must be non-empty when legal hold is activated"
  );

  // Should throw when activating with whitespace-only reason
  await expect(client.setLegalHold(true, "   ")).rejects.toThrow(
    "reason must be non-empty when legal hold is activated"
  );

  // Should not throw when deactivating with empty reason
  const mockInvoke = jest.fn().mockResolvedValue(undefined);
  rpc.invoke = mockInvoke;
  client.setContractId("CTEST123");
  await client.setLegalHold(false, "");
  expect(mockInvoke).toHaveBeenCalled();
});
