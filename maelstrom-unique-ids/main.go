package main

import (
    "log"
    // "time"
    // "sync"
    // "math/rand"

    "github.com/google/uuid"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// type Node struct {
//     *maelstrom.Node
//     lastTimestamp int64
//     serial        int64
//     machineID     int64
//     mu            sync.Mutex
//     // lets define the shifts
//     timestampShift uint64
//     machineIDShift uint64
//     serialMask     int64
// }

// func (n *Node) GenerateID() int64 {
//     n.mu.Lock()
//     defer n.mu.Unlock()
//     // Get current timestamp in milliseconds
//     timestamp := time.Now().UnixNano() / 1e6

//     // If the timestamp is the same as the last one, increment the serial
//     if timestamp == n.lastTimestamp {
//         n.serial = (n.serial + 1) & n.serialMask
//         if n.serial == 0 {
//             // If the serial overflows, wait until the next millisecond
//             for timestamp <= n.lastTimestamp {
//                 timestamp = time.Now().UnixNano() / 1e6
//             }
//         }
//     } else {
//         n.serial = 0
//     }

//     // Update the last timestamp
//     n.lastTimestamp = timestamp

//     // Generate the Snowflake-like ID
//     id := (timestamp << n.timestampShift) | (n.machineID << n.machineIDShift) | n.serial
//     return id
// }

// func main() {
//     maelstrom_node := maelstrom.NewNode()
//     n := &Node{
//         Node:          maelstrom_node,
//         lastTimestamp: 0,
//         serial:        0,
//         machineID:   rand.Int63n(1 << 10),
//         timestampShift: 22,
//         machineIDShift: 12,
//         serialMask:    -1 ^ (-1 << 12),
//     }
//     n.Node.Handle("generate", func(msg maelstrom.Message) error {
//         // Generate a unique Snowflake-like ID
//         id := n.GenerateID()

//         // Prepare response body
//         response := make(map[string]interface{})
//         response["type"] = "generate_ok"
//         response["id"] = id

//         // Send response
//         return n.Node.Reply(msg, response)
//     })

//     if err := n.Node.Run(); err != nil {
//         log.Fatal(err)
//     }
// }

func main() {
    n := maelstrom.NewNode()

    n.Handle("generate", func(msg maelstrom.Message) error {
        // Generate a UUID (using Google's UUID library)
        id := uuid.New()

        // Prepare response body
        response := make(map[string]interface{})
        response["type"] = "generate_ok"
        response["id"] = id

        // Send response
        return n.Reply(msg, response)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}