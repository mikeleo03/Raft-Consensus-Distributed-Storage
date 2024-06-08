interface address {
    ip: string;
    port: number;
}

// To set the request
export interface KeyValueRequest {
    address: address;
    command: string;
}

// To set the value response value
export interface KeyValueResponse {
    address: address;
    data: string;
    status: string;
}