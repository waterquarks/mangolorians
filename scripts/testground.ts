import { PerpEventLayout } from '@blockworks-foundation/mango-client';
import WebSocket from 'ws'

const ws = new WebSocket('ws://api.mngo.cloud:8080')

ws.onmessage = (message) => {
    const { data } = message

    const perpEvent = JSON.parse(data.toString());

    // `events` is received when the connection is first established, containing a snapshot of past fills
    // `event` is received in subsequent messages, containing one fill at a time
    const isSnapshot = perpEvent.hasOwnProperty('events')

    const parse = (event: string) =>
        PerpEventLayout.decode(Buffer.from(event, 'base64'))

    if (isSnapshot) {
        console.log(perpEvent.market)
        console.log(perpEvent.events.length)
        // for (const event of perpEvent.events) {
        //     console.log(parse(event))
        // }
    } else {
        console.log(parse(perpEvent.event))
    }
}
