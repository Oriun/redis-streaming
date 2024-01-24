/// <reference types="node" />
export declare class AsyncIterableStream<T = Buffer> implements AsyncIterable<T> {
    private reader;
    private stream;
    constructor(props: UnderlyingDefaultSource<T>);
    next(): Promise<IteratorResult<T, any>>;
    return(): Promise<IteratorResult<T, T[]>>;
    [Symbol.asyncIterator](): this;
}
export interface ClientOptions {
    dev?: boolean;
}
export declare const defaultOptions: ClientOptions;
export declare function createClient(port: number, host: string, options?: ClientOptions): {
    write(command: string): void;
    close(): void;
    get(key: string): Promise<AsyncIterableStream<Buffer>>;
    keys(key?: string): Promise<AsyncIterableStream<Buffer>>;
    set(key: string, value: string): Promise<boolean>;
    del(key: string): Promise<number>;
    block(key: string): Promise<boolean>;
};
export default createClient;
