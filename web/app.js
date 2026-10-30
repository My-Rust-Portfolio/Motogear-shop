const CUSTOMER_STORAGE_KEY = "motogear.customerId";
const PRODUCT_POLL_INTERVAL_MS = 2000;

const customerSelect = document.querySelector("#customer-select");
const customerStatus = document.querySelector("#customer-status");
const connectionIndicator = document.querySelector("#connection-indicator");
const connectionStatus = document.querySelector("#connection-status");
const catalogueRefresh = document.querySelector("#catalogue-refresh");
const productsContainer = document.querySelector("#products");
const emptyState = document.querySelector("#empty-state");
const cartCount = document.querySelector("#cart-count");
const cartTotal = document.querySelector("#cart-total");
const cartEmpty = document.querySelector("#cart-empty");
const cartItemsContainer = document.querySelector("#cart-items");

const eurFormatter = new Intl.NumberFormat(undefined, {
  style: "currency",
  currency: "EUR",
});

let productRequestInFlight = false;
let hasProductSnapshot = false;
let lastSuccessfulRefresh = null;
const cart = new Map();
const quantityChoices = new Map();

class ApiRequestError extends Error {
  constructor(message) {
    super(message);
    this.name = "ApiRequestError";
  }
}

async function fetchJson(url, resourceName) {
  let response;
  try {
    response = await fetch(url, {
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
  } catch {
    throw new ApiRequestError(`The API could not be reached while loading ${resourceName}.`);
  }

  if (!response.ok) {
    const reason = response.status === 400
      ? "the request was rejected"
      : response.status >= 500
        ? "the server could not complete the request"
        : "the request failed";
    throw new ApiRequestError(`The ${resourceName} request failed: ${reason} (HTTP ${response.status}).`);
  }

  try {
    return await response.json();
  } catch {
    throw new ApiRequestError(`The API returned invalid JSON while loading ${resourceName}.`);
  }
}

function readSavedCustomerId() {
  try {
    return sessionStorage.getItem(CUSTOMER_STORAGE_KEY);
  } catch {
    return null;
  }
}

function saveCustomerId(customerId) {
  try {
    sessionStorage.setItem(CUSTOMER_STORAGE_KEY, customerId);
  } catch {
    // The selector remains usable even if this browser blocks session storage.
  }
}

function setConnectionState(connected) {
  connectionIndicator.classList.toggle("connected", connected);
  connectionIndicator.classList.toggle("disconnected", !connected);
  connectionStatus.textContent = connected ? "Connected" : "Connection unavailable";
}

async function loadCustomers() {
  try {
    const customers = await fetchJson("/api/v1/customers?limit=100&offset=0", "customers");
    if (!Array.isArray(customers)) {
      throw new ApiRequestError("The API returned an invalid customer list.");
    }
    const savedCustomerId = readSavedCustomerId();
    customerSelect.replaceChildren();

    for (const customer of customers) {
      const option = document.createElement("option");
      option.value = customer.id;
      option.textContent = customer.name;
      customerSelect.append(option);
    }

    if (customers.length === 0) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No customers available";
      customerSelect.append(option);
      customerSelect.disabled = true;
      customerStatus.textContent = "The API returned no demo customers.";
      return;
    }

    const savedCustomerExists = customers.some((customer) => customer.id === savedCustomerId);
    customerSelect.value = savedCustomerExists ? savedCustomerId : customers[0].id;
    customerSelect.disabled = false;
    saveCustomerId(customerSelect.value);
    customerStatus.textContent = "Saved for this tab; checkout is not connected yet.";
  } catch (error) {
    customerSelect.replaceChildren();
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "Customers unavailable";
    customerSelect.append(option);
    customerSelect.disabled = true;
    customerStatus.textContent = error instanceof Error
      ? error.message
      : "Customers could not be loaded.";
  }
}

function createProductCard(product) {
  const card = document.createElement("article");
  card.className = "product-card";

  const sku = document.createElement("span");
  sku.className = "product-sku";
  sku.textContent = product.sku;

  const name = document.createElement("h3");
  name.textContent = product.name;

  const description = document.createElement("p");
  description.className = "product-description";
  description.textContent = product.description;

  const meta = document.createElement("div");
  meta.className = "product-meta";

  const price = document.createElement("span");
  price.className = "product-price";
  price.textContent = eurFormatter.format(product.price_cents / 100);

  const stock = document.createElement("span");
  stock.className = "stock-pill";
  if (product.stock_available === 0) {
    stock.classList.add("out-of-stock");
    stock.textContent = "Out of stock";
  } else {
    stock.textContent = `${product.stock_available} available`;
  }

  meta.append(price, stock);

  const actions = document.createElement("div");
  actions.className = "product-actions";

  const quantityLabel = document.createElement("label");
  quantityLabel.className = "quantity-picker";
  quantityLabel.textContent = "Quantity";

  const quantityInput = document.createElement("input");
  quantityInput.type = "number";
  quantityInput.min = "1";
  quantityInput.max = String(Math.max(1, product.stock_available));
  quantityInput.step = "1";
  quantityInput.value = quantityChoices.get(product.id) ?? "1";
  quantityInput.disabled = product.stock_available === 0;
  quantityInput.setAttribute("aria-label", `Quantity for ${product.name}`);
  quantityInput.addEventListener("input", () => quantityChoices.set(product.id, quantityInput.value));
  quantityLabel.append(quantityInput);

  const addButton = document.createElement("button");
  addButton.className = "add-to-cart";
  addButton.type = "button";
  addButton.textContent = "Add to cart";
  addButton.disabled = product.stock_available === 0;

  const feedback = document.createElement("p");
  feedback.className = "cart-feedback";
  feedback.setAttribute("aria-live", "polite");

  addButton.addEventListener("click", () => {
    const quantity = quantityInput.valueAsNumber;
    if (!Number.isInteger(quantity) || quantity < 1) {
      feedback.textContent = "Enter a whole quantity of at least 1.";
      return;
    }

    const existing = cart.get(product.id);
    const alreadyInCart = existing?.quantity ?? 0;
    if (alreadyInCart + quantity > product.stock_available) {
      feedback.textContent = `Only ${product.stock_available} available; ${alreadyInCart} already in your cart.`;
      return;
    }

    cart.set(product.id, { product, quantity: alreadyInCart + quantity });
    quantityChoices.set(product.id, "1");
    renderCart();
    renderProducts(currentProducts);
  });

  actions.append(quantityLabel, addButton);
  card.append(sku, name, description, meta, actions, feedback);
  return card;
}

let currentProducts = [];

function renderProducts(products) {
  currentProducts = products;
  const fragment = document.createDocumentFragment();
  for (const product of products) fragment.append(createProductCard(product));
  productsContainer.replaceChildren(fragment);
  emptyState.hidden = products.length !== 0;
  productsContainer.hidden = products.length === 0;
}

function renderCart() {
  const entries = [...cart.values()];
  const itemCount = entries.reduce((sum, item) => sum + item.quantity, 0);
  const totalCents = entries.reduce(
    (sum, item) => sum + item.product.price_cents * item.quantity,
    0,
  );
  const fragment = document.createDocumentFragment();

  for (const { product, quantity } of entries) {
    const item = document.createElement("li");
    item.className = "cart-item";

    const details = document.createElement("div");
    details.className = "cart-item-details";
    const name = document.createElement("span");
    name.className = "cart-item-name";
    name.textContent = product.name;
    const unitPrice = document.createElement("span");
    unitPrice.className = "cart-item-price";
    unitPrice.textContent = `${product.sku} · ${eurFormatter.format(product.price_cents / 100)} each`;
    details.append(name, unitPrice);

    const actions = document.createElement("div");
    actions.className = "cart-item-actions";
    const lineTotal = document.createElement("span");
    lineTotal.className = "cart-item-line-total";
    lineTotal.textContent = `${quantity} × ${eurFormatter.format(product.price_cents / 100)} = ${eurFormatter.format((product.price_cents * quantity) / 100)}`;

    const removeButton = document.createElement("button");
    removeButton.className = "cart-remove";
    removeButton.type = "button";
    removeButton.textContent = "Remove";
    removeButton.setAttribute("aria-label", `Remove ${product.name} from cart`);
    removeButton.addEventListener("click", () => {
      cart.delete(product.id);
      renderCart();
    });

    actions.append(lineTotal, removeButton);
    item.append(details, actions);
    fragment.append(item);
  }

  cartCount.textContent = `(${itemCount})`;
  cartTotal.textContent = eurFormatter.format(totalCents / 100);
  cartItemsContainer.replaceChildren(fragment);
  cartEmpty.hidden = entries.length !== 0;
  cartItemsContainer.hidden = entries.length === 0;
}

function formatRefreshTime(date) {
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

async function refreshProducts() {
  if (productRequestInFlight || document.hidden) return;
  productRequestInFlight = true;
  productsContainer.setAttribute("aria-busy", "true");

  try {
    const products = await fetchJson("/api/v1/products?limit=100&offset=0", "products");
    if (!Array.isArray(products)) {
      throw new ApiRequestError("The API returned an invalid product list.");
    }

    const refreshedProducts = new Map(products.map((product) => [product.id, product]));
    for (const [productId, item] of cart) {
      const refreshedProduct = refreshedProducts.get(productId);
      item.product = refreshedProduct ?? { ...item.product, stock_available: 0 };
    }
    renderProducts(products);
    renderCart();
    lastSuccessfulRefresh = new Date();
    hasProductSnapshot = true;
    setConnectionState(true);
    catalogueRefresh.textContent = `Stock updated at ${formatRefreshTime(lastSuccessfulRefresh)}`;
  } catch (error) {
    const reason = error instanceof Error ? error.message : "Products could not be loaded.";
    setConnectionState(false);
    if (hasProductSnapshot) {
      catalogueRefresh.textContent = `Refresh failed: ${reason} Showing stock from ${formatRefreshTime(lastSuccessfulRefresh)}.`;
    } else {
      catalogueRefresh.textContent = `Catalogue could not be loaded: ${reason}`;
      productsContainer.textContent = "Products are temporarily unavailable.";
    }
  } finally {
    productRequestInFlight = false;
    productsContainer.setAttribute("aria-busy", "false");
  }
}

customerSelect.addEventListener("change", () => saveCustomerId(customerSelect.value));
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refreshProducts();
});

loadCustomers();
refreshProducts();
window.setInterval(refreshProducts, PRODUCT_POLL_INTERVAL_MS);
