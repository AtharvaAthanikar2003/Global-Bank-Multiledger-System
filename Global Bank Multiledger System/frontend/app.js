const API_BASE = "http://localhost:8000";

/* ================= TOAST ================= */

function showToast(message, type = "success") {
    const toast = document.getElementById("toast");

    toast.textContent = message;
    toast.className = `show ${type}`;

    setTimeout(() => {
        toast.className = "";
    }, 500);
}

/* ================= VALIDATION ================= */

function getValidatedInputs(requireAmount = false) {
    const userId = Number(document.getElementById("userId").value);
    const currency = document.getElementById("currency").value;
    const amount = Number(document.getElementById("amount").value);

    if (!userId || userId < 1)
        throw new Error("Invalid User ID");

    if (requireAmount && (!amount || amount <= 0))
        throw new Error("Invalid Amount");

    return { userId, currency, amount };
}

/* ================= CURRENCY FORMAT ================= */

function formatCurrency(currency, amount) {
    const symbols = {
        INR: "₹",
        USD: "$",
        CAD: "C$",
        GBP: "£",
        EUR: "€",
        DKK: "kr ",
        CHF: "CHF ",
        AUD: "A$",
        NZD: "NZ$",
        SGD: "S$",
        RUB: "₽",
        JPY: "¥",
        CNY: "¥",
        AED: "د.إ ",
        KWD: "د.ك ",
        BHD: "ب.د ",
        JOD: "د.ا ",
        OMR: "ر.ع ",
        BRL: "R$",
        ZAR: "R "
    };

    const symbol = symbols[currency] || (currency + " ");
    return symbol + Number(amount).toFixed(2);
}

/* ================= SUMMARY RENDER ================= */

function renderSummary(userId, currency, balance) {

    document.getElementById("summaryUser").textContent =
        `User ID: ${userId}`;

    document.getElementById("balanceCurrency").textContent =
        `Available Balance (${currency})`;

    document.getElementById("balanceAmount").textContent =
        formatCurrency(currency, balance);

    const box = document.querySelector(".balance-box");

    /* Remove old theme */
    box.className = "balance-box";

    /* Add new theme */
    box.classList.add(`balance-${currency}`);
}

function safeNumber(value) {
    const num = Number(value);

    if (!num) return "0.00";   // Handles 0, null, undefined, NaN

    return num.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

/* ================= TRANSACTIONS RENDER ================= */

function renderTransactions(transactions) {
    const tbody = document.querySelector("#txnTable tbody");

    tbody.innerHTML = "";

    if (!transactions || !transactions.length) {
        tbody.innerHTML =
            `<tr><td colspan="7">No Transactions</td></tr>`;
        return;
    }

    transactions.forEach(txn => {

        tbody.innerHTML += `
            <tr>
                <td><strong>${txn.user_id ?? "-"}</strong></td>
                <td>${txn.from ?? "-"}</td>
                <td>${txn.to ?? "-"}</td>
                <td><strong>${safeNumber(txn.prev_balance)}</strong></td>
                <td>${safeNumber(txn.debit)}</td>
                <td>${safeNumber(txn.credit)}</td>
                <td><strong>${safeNumber(txn.new_balance)}</strong></td>
            </tr>
        `;
    });
}

/* ================= LOAD TRANSACTIONS ================= */

async function loadTransactions(userId) {

    try {
        const res = await fetch(`${API_BASE}/transactions/${userId}`);
        const data = await res.json();

        renderTransactions(data.transactions);

    } catch (err) {
        console.error("Transaction Load Error:", err);
    }
}

/* ================= FETCH BALANCE ================= */

async function fetchBalance(userId, currency) {

    const res = await fetch(`${API_BASE}/balance/${userId}`);
    const data = await res.json();

    const wallet = data.wallets.find(w => w.currency === currency);

    return wallet ? wallet.balance : 0;
}

/* ================= CHECK BALANCE ================= */

async function getBalance() {
    try {
        const { userId, currency } = getValidatedInputs();

        const balance = await fetchBalance(userId, currency);

        renderSummary(userId, currency, balance);
        await loadTransactions(userId);

        showToast("Balance Loaded");

    } catch (err) {
        showToast(err.message, "error");
    }
}

/* ================= DEPOSIT ================= */

async function deposit() {
    try {
        const { userId, currency, amount } =
            getValidatedInputs(true);

        const res = await fetch(`${API_BASE}/deposit`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                user_id: userId,
                currency,
                amount
            })
        });

        const data = await res.json();

        if (!res.ok)
            throw new Error(data.detail || "Deposit Failed");

        renderSummary(userId, currency, data.new_balance);
        await loadTransactions(userId);

        showToast("Deposit Successful");

    } catch (err) {
        showToast(err.message, "error");
    }
}

/* ================= WITHDRAW ================= */

async function withdraw() {
    try {
        const { userId, currency, amount } =
            getValidatedInputs(true);

        const res = await fetch(`${API_BASE}/withdraw`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                user_id: userId,
                currency,
                amount
            })
        });

        const data = await res.json();

        if (!res.ok)
            throw new Error(data.detail || "Withdrawal Failed");

        renderSummary(userId, currency, data.new_balance);
        await loadTransactions(userId);

        showToast("Withdrawal Successful");

    } catch (err) {
        showToast(err.message, "error");
    }
}