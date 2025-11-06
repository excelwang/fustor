// --- API CONFIGURATION ---
// !!! 重要 !!!
// 请在此处配置您后端API的基地址.
const BASE_API_URL = window.location.origin; // 例如: 'https://api.example.com'

// --- API HELPER ---
/**
 * A helper function to make authenticated API requests.
 * @param {string} endpoint The API endpoint to call.
 * @param {object} options The options for the fetch request.
 * @returns {Promise<any>} The JSON response from the API.
 */
const apiFetch = async (endpoint, options = {}) => {
    const token = localStorage.getItem('authToken');
    const headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        ...options.headers,
    };

    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    }

    const response = await fetch(`${BASE_API_URL}${endpoint}`, {
        ...options,
        headers,
    });

    if (!response.ok) {
        let errorDetail = '未知错误';
        try {
            const errorData = await response.json();
            if (errorData && errorData.detail) {
                errorDetail = errorData.detail;
            } else if (typeof errorData === 'string') {
                errorDetail = errorData;
            }
        } catch (e) {
            // If response is not JSON, use status text or a generic message
            errorDetail = response.statusText || `HTTP 错误! 状态: ${response.status}`;
        }
        console.error('API Error:', response.status, errorDetail);
        throw new Error(errorDetail);
    }
    
    if (response.status === 204) {
        return null;
    }

    return response.json();
};


// --- API SERVICE ---
// This service handles all communication with the backend API.
const ApiService = {
    login: async (username, password) => {
        const params = new URLSearchParams();
        params.append('username', username);
        params.append('password', password);
        
        const response = await fetch(`${BASE_API_URL}/v1/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params,
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => null);
            throw new Error(errorData?.detail || '登录失败，请检查您的凭据');
        }

        const data = await response.json();
        localStorage.setItem('authToken', data.access_token);
        return data;
    },
    logout: async () => {
         try {
            await apiFetch('/v1/auth/logout', { method: 'GET' });
        } catch(error){
           console.error("Logout API call failed, but logging out client-side.", error);
        } finally {
            localStorage.removeItem('authToken');
        }
    },
    getProfile: () => apiFetch('/v1/profile/'),
    updateProfile: (data) => apiFetch('/v1/profile/', { method: 'PUT', body: JSON.stringify(data) }),
    changePassword: (data) => apiFetch('/v1/profile/password', { method: 'PUT', body: JSON.stringify(data) }),
    
    getDatastores: () => apiFetch('/v1/datastores/'),
    getDatastore: (id) => apiFetch(`/v1/datastores/${id}`),
    createDatastore: (data) => apiFetch('/v1/datastores/', { method: 'POST', body: JSON.stringify(data) }),
    updateDatastore: (id, data) => apiFetch(`/v1/datastores/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
    deleteDatastore: (id) => apiFetch(`/v1/datastores/${id}`, { method: 'DELETE' }),

    getApiKeys: () => apiFetch('/v1/keys/'),
    createApiKey: (data) => apiFetch('/v1/keys/', { method: 'POST', body: JSON.stringify(data) }),
    deleteApiKey: (id) => apiFetch(`/v1/keys/${id}`, { method: 'DELETE' }),

    // --- Admin User Management ---
    listUsers: () => apiFetch('/v1/admin/users'),
    createUser: (data) => apiFetch('/v1/admin/users', { method: 'POST', body: JSON.stringify(data) }),
    deleteUser: (userId) => apiFetch(`/v1/admin/${userId}`, { method: 'DELETE' }),
};