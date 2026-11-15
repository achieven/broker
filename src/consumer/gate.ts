/**
 * HALT gate, fail closed: `true` means halted. Read once at start, then polled in the background.
 * A thrown error counts as halted, so a broken gate can never let messages through.
 */
export class Gate {
    private halted = true;
    private stopped = false;
    private timer?: NodeJS.Timeout;
    private wake?: () => void;

    constructor(private readonly read: () => Promise<boolean>, private readonly pollMs = 1000) {}

    get isHalted(): boolean {
        return this.halted;
    }

    /** First read (so the consumer never processes before knowing the state), then the background poll. */
    async start(): Promise<void> {
        this.halted = await this.check();
        void this.loop();
    }

    stop(): void {
        this.stopped = true;
        if (this.timer) clearTimeout(this.timer);
        this.wake?.();
    }

    private async check(): Promise<boolean> {
        try {
            return (await this.read()) !== false;
        } catch {
            return true;
        }
    }

    private async loop(): Promise<void> {
        while (!this.stopped) {
            await new Promise<void>((resolve) => {
                this.wake = resolve;
                this.timer = setTimeout(resolve, this.pollMs);
                this.timer.unref();
            });
            if (this.stopped) return;
            this.halted = await this.check();
        }
    }
}
