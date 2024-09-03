export class WebSocketConnection {
  #endpoint: string;
  #ws!: WebSocket;
  #onOpen: (send: (data: Uint8Array) => void, reason?: string) => void;
  #onMessage: (data: Uint8Array) => void;
  #closed = true;
  constructor(
    endpoint: string,
    onOpen: (send: (data: Uint8Array) => void, reason?: string) => void,
    onMessage: (data: Uint8Array) => void
  ) {
    this.#endpoint = endpoint;
    this.#onOpen = onOpen;
    this.#onMessage = onMessage;
  }

  #init(reason?: string) {
    this.#ws = new WebSocket(this.#endpoint);
    this.#ws.binaryType = "arraybuffer";
    this.#closed = false;
    this.#ws.onopen = () => {
      this.#onOpen((data) => this.#ws.send(data), reason);
    };
    this.#ws.onmessage = (e) => {
      this.#onMessage(new Uint8Array(e.data));
    };
    this.#ws.onerror = (e) => {
      this.#ws.close();
    };
    this.#ws.onclose = (e) => {
      if (!this.#closed) {
        this.#init(e.reason);
      }
    };
  }

  trySend(lazy: () => Uint8Array) {
    if (this.#ws?.readyState === WebSocket.OPEN) {
      this.#ws.send(lazy());
    }
  }

  open() {
    if (!this.#closed) return;
    this.#init();
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#ws.close();
  }
}
