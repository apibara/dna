/* PostgreSQL Entity mode example
 *
 * This example shows how to use entity mode.
 * It listens for VRF requests and status changes from the Pragma's smart
 * contract on testnet.
 */

const fromAddress =
  "0x693d551265f0be7ccb3c869c64b5920929caaf486497788d43cb37dd17d6be6";

// RandomnessRequest event selector
const requestSelector =
  "0x00e3e1c077138abb6d570b1a7ba425f5479b12f50a78a72be680167d4cf79c48";

// RandomnessStatusChange event selector
const statusChangeSelector =
  "0x015510b399942790499934b72bc68b883f0905dee5da5aa36e489c9ffb096b8c";

export const config = {
  streamUrl: "https://goerli.starknet.a5a.ch",
  startingBlock: 908_100,
  network: "starknet",
  batchSize: 1,
  finality: "DATA_STATUS_ACCEPTED",
  filter: {
    header: { weak: true },
    events: [
      {
        fromAddress,
        keys: [requestSelector],
        includeTransaction: true,
        includeReceipt: false,
      },
      {
        fromAddress,
        keys: [statusChangeSelector],
        includeTransaction: true,
        includeReceipt: false,
      },
    ],
  },
  sinkType: "postgres",
  sinkOptions: {
    tableName: "vrf_requests",
    entityMode: true,
  },
};

export default function transform({ header, events }) {
  const { timestamp } = header;
  return events.flatMap(({ event, transaction }) => {
    if (event.keys[0] == requestSelector) {
      // Initialize request entity.
      const requestId = BigInt(event.data[0]);
      return {
        insert: {
          request_id: +requestId.toString(),
          created_at: timestamp,
          created_at_tx: transaction.meta.hash,
          updated_at: timestamp,
          updated_at_tx: transaction.meta.hash,
          status: 0,
        },
      };
    } else if (event.keys[0] == statusChangeSelector) {
      // Update request entity.
      const requestId = BigInt(event.data[1]);
      const status = BigInt(event.data[2]);
      return {
        entity: {
          request_id: +requestId.toString(),
        },
        update: {
          status: +status.toString(),
          updated_at: timestamp,
          updated_at_tx: transaction.meta.hash,
        },
      };
    } else {
      // Do nothing.
      return [];
    }
  });
}

