import axios from "axios";

import { KeyValueRequest, KeyValueResponse } from "@/types";

class MainApi {
    private static axios = axios.create({
        baseURL: "http://localhost:5000",
        headers: {
            "Content-Type": "application/json",
        },
    });

    static async request(payload: KeyValueRequest): Promise<KeyValueResponse> {
        try {
            const response = await this.axios.post<KeyValueResponse>("/execute_command", payload);
            return response.data;
        } catch (error) {
            throw error;
        }
    }
}

export default MainApi;