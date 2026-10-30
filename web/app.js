const CUSTOMER_STORAGE_KEY = "motogear.customerId";
const PENDING_CHECKOUT_STORAGE_KEY = "motogear.pendingCheckout";
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
const cartNote = document.querySelector("#cart-note");
const checkoutButton = document.querySelector("#checkout-button");
const checkoutStatus = document.querySelector("#checkout-status");

const eurFormatter = new Intl.NumberFormat(undefined, {
  style: "currency",
  currency: "EUR",
});

let productRequestInFlight = false;
let hasProductSnapshot = false;
let lastSuccessfulRefresh = null;
let checkoutInFlight = false;
let customersAvailable = false;
const cart = new Map();
const quantityChoices = new Map();
let pendingCheckout = readPendingCheckout();
let pendingCheckoutStored = pendingCheckout !== null;
let pendingCartRestored = false;

class ApiRequestError extends Error {
  constructor(message) {
    super(message);
    this.name = "ApiRequestError";
  }
}

function isUuid(value) {
  return typeof value === "string"
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function isCustomer(value) {
  return value !== null
    && typeof value === "object"
    && isUuid(value.id)
    && typeof value.name === "string";
}

function isProduct(value) {
  return value !== null
    && typeof value === "object"
    && isUuid(value.id)
    && typeof value.sku === "string"
    && typeof value.name === "string"
    && typeof value.description === "string"
    && Number.isSafeInteger(value.price_cents)
    && value.price_cents >= 0
    && Number.isSafeInteger(value.stock_available)
    && value.stock_available >= 0;
}

function isValidPendingCheckout(value) {
  return value !== null
    && typeof value === "object"
    && isUuid(value.key)
    && value.request !== null
    && typeof value.request === "object"
    && isUuid(value.request.customer_id)
    && Array.isArray(value.request.items)
    && value.request.items.length > 0
    && value.request.items.length <= 50
    && value.request.items.every((item) => item !== null
      && typeof item === "object"
      && isUuid(item.product_id)
      && Number.isInteger(item.quantity)
      && item.quantity >= 1
      && item.quantity <= 1000);
}

function readPendingCheckout() {
  try {
    const saved = sessionStorage.getItem(PENDING_CHECKOUT_STORAGE_KEY);
    if (!saved) return null;
    const pending = JSON.parse(saved);
    if (isValidPendingCheckout(pending)) return pending;
    sessionStorage.removeItem(PENDING_CHECKOUT_STORAGE_KEY);
  } catch {
    // Checkout still works in memory if this browser blocks session storage.
  }
  return null;
}

function savePendingCheckout(pending) {
  try {
    sessionStorage.setItem(PENDING_CHECKOUT_STORAGE_KEY, JSON.stringify(pending));
    pendingCheckoutStored = true;
  } catch {
    pendingCheckoutStored = false;
  }
}

function clearPendingCheckout() {
  pendingCheckout = null;
  pendingCheckoutStored = false;
  pendingCartRestored = false;
  try {
    sessionStorage.removeItem(PENDING_CHECKOUT_STORAGE_KEY);
  } catch {
    // A stale saved key can only replay the same request, which remains safe.
  }
}

function createIdempotencyKey() {
  const cryptoApi = globalThis.crypto;
  if (typeof cryptoApi?.randomUUID === "function") return cryptoApi.randomUUID();
  if (!cryptoApi?.getRandomValues) throw new Error("Secure random numbers are unavailable.");
  const bytes = cryptoApi.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function checkoutLocked() {
  return checkoutInFlight || pendingCheckout !== null;
}

function updateCheckoutControls() {
  const locked = checkoutLocked();
  customerSelect.disabled = !customersAvailable || locked;
  checkoutButton.disabled = checkoutInFlight
    || (!pendingCheckout && (!customersAvailable || !customerSelect.value || cart.size === 0));
  checkoutButton.textContent = checkoutInFlight
    ? "Submitting order…"
    : pendingCheckout
      ? "Retry saved checkout"
      : "Place order";
  cartNote.textContent = pendingCheckout
    ? "A saved checkout is awaiting confirmation. Retry it safely before changing your selection."
    : "Prices are estimates. Checkout uses current catalogue prices and does not reserve stock.";
}

function setCheckoutStatus(message, kind = "") {
  checkoutStatus.textContent = message;
  checkoutStatus.classList.toggle("error", kind === "error");
  checkoutStatus.classList.toggle("success", kind === "success");
}

async function fetchJson(url, resourceName) {
  let response;
  try {
    response = await fetch(url, {
      headers: { Accept: "application/json" },
      cache: "no-store",
      signal: AbortSignal.timeout(10_000),
    });
  } catch (error) {
    if (error?.name === "TimeoutError") {
      throw new ApiRequestError(`Loading ${resourceName} timed out after 10 seconds.`);
    }
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
    if (!Array.isArray(customers) || !customers.every(isCustomer)) {
      throw new ApiRequestError("The API returned an invalid customer list.");
    }
    const savedCustomerId = pendingCheckout?.request.customer_id ?? readSavedCustomerId();
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
      customersAvailable = false;
      customerStatus.textContent = "The API returned no demo customers.";
      updateCheckoutControls();
      return;
    }

    const savedCustomerExists = customers.some((customer) => customer.id === savedCustomerId);
    customerSelect.value = savedCustomerExists ? savedCustomerId : customers[0].id;
    customersAvailable = true;
    saveCustomerId(customerSelect.value);
    customerStatus.textContent = "Checkout uses this customer; choice is saved for this tab.";
    updateCheckoutControls();
  } catch (error) {
    customerSelect.replaceChildren();
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "Customers unavailable";
    customerSelect.append(option);
    customersAvailable = false;
    customerStatus.textContent = error instanceof Error
      ? error.message
      : "Customers could not be loaded.";
    setConnectionState(false);
    updateCheckoutControls();
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
  quantityInput.max = String(Math.max(1, Math.min(1000, product.stock_available)));
  quantityInput.step = "1";
  quantityInput.value = quantityChoices.get(product.id) ?? "1";
  quantityInput.disabled = product.stock_available === 0 || checkoutLocked();
  quantityInput.setAttribute("aria-label", `Quantity for ${product.name}`);
  quantityInput.addEventListener("input", () => quantityChoices.set(product.id, quantityInput.value));
  quantityLabel.append(quantityInput);

  const addButton = document.createElement("button");
  addButton.className = "add-to-cart";
  addButton.type = "button";
  addButton.textContent = "Add to cart";
  addButton.disabled = product.stock_available === 0 || checkoutLocked();

  const feedback = document.createElement("p");
  feedback.className = "cart-feedback";
  feedback.setAttribute("aria-live", "polite");

  addButton.addEventListener("click", () => {
    if (checkoutLocked()) return;
    const quantity = quantityInput.valueAsNumber;
    if (!Number.isInteger(quantity) || quantity < 1) {
      feedback.textContent = "Enter a whole quantity of at least 1.";
      return;
    }

    const existing = cart.get(product.id);
    const alreadyInCart = existing?.quantity ?? 0;
    if (alreadyInCart + quantity > product.stock_available || alreadyInCart + quantity > 1000) {
      const available = Math.min(product.stock_available, 1000);
      feedback.textContent = `You can add up to ${available} total for this product.`;
      return;
    }

    cart.set(product.id, {
      product,
      quantity: alreadyInCart + quantity,
      availability: product.stock_available === 0 ? "out-of-stock" : "available",
    });
    quantityChoices.set(product.id, "1");
    setCheckoutStatus("");
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

  for (const itemData of entries) {
    const { product, quantity } = itemData;
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
    const availability = document.createElement("span");
    availability.className = `cart-item-availability ${itemData.availability ?? "available"}`;
    if (itemData.availability === "no-longer-available") {
      availability.textContent = "No longer available in the current catalogue.";
    } else if (itemData.availability === "previously-selected") {
      availability.textContent = "Previously selected item; not in the current catalogue.";
    } else if (itemData.availability === "out-of-stock") {
      availability.textContent = "Out of stock.";
    } else {
      availability.textContent = "Available.";
    }
    details.append(name, unitPrice, availability);

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
    removeButton.disabled = checkoutLocked();
    removeButton.addEventListener("click", () => {
      if (checkoutLocked()) return;
      cart.delete(product.id);
      setCheckoutStatus("");
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
  updateCheckoutControls();
}

function restorePendingCart(productsById) {
  if (!pendingCheckout || pendingCartRestored) return;
  const savedItems = new Map(
    (Array.isArray(pendingCheckout.displayItems) ? pendingCheckout.displayItems : [])
      .map((item) => [item.id, item]),
  );

  for (const requestedItem of pendingCheckout.request.items) {
    const currentProduct = productsById.get(requestedItem.product_id);
    const savedProduct = savedItems.get(requestedItem.product_id);
    const product = currentProduct ?? {
      id: requestedItem.product_id,
      sku: savedProduct?.sku ?? "Previously selected item",
      name: savedProduct?.name ?? "Previously selected item",
      description: savedProduct?.description ?? "",
      price_cents: savedProduct?.price_cents ?? 0,
      stock_available: 0,
    };
    cart.set(requestedItem.product_id, {
      product,
      quantity: requestedItem.quantity,
      availability: currentProduct
        ? currentProduct.stock_available === 0 ? "out-of-stock" : "available"
        : "previously-selected",
    });
  }

  pendingCartRestored = true;
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
    if (!Array.isArray(products) || !products.every(isProduct)) {
      throw new ApiRequestError("The API returned an invalid product list.");
    }

    const refreshedProducts = new Map(products.map((product) => [product.id, product]));
    restorePendingCart(refreshedProducts);
    for (const [productId, item] of cart) {
      const refreshedProduct = refreshedProducts.get(productId);
      if (refreshedProduct) {
        item.product = refreshedProduct;
        item.availability = refreshedProduct.stock_available === 0 ? "out-of-stock" : "available";
      } else {
        item.product = { ...item.product, stock_available: 0 };
        item.availability = item.availability === "previously-selected"
          ? "previously-selected"
          : "no-longer-available";
      }
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

function createPendingCheckout() {
  const request = {
    customer_id: customerSelect.value,
    items: [...cart.values()].map(({ product, quantity }) => ({
      product_id: product.id,
      quantity,
    })),
  };
  const displayItems = [...cart.values()].map(({ product }) => ({
    id: product.id,
    sku: product.sku,
    name: product.name,
    description: product.description,
    price_cents: product.price_cents,
  }));
  pendingCheckout = {
    key: createIdempotencyKey(),
    request, // This is the only object sent to the API.
    displayItems, // Browser-only data for restoring readable cart labels after reload.
  };
  savePendingCheckout(pendingCheckout);
}

function pendingRetryGuidance() {
  return pendingCheckoutStored
    ? "Retrying sends the same saved request and key."
    : "Retrying in this tab reuses the same key, but browser storage is unavailable. Keep this tab open.";
}

async function responseErrorMessage(response) {
  try {
    const body = await response.json();
    if (typeof body.error?.message === "string") return body.error.message;
  } catch {
    // Fall back to the status code when the server did not return its error envelope.
  }
  return `The API returned HTTP ${response.status}.`;
}

async function submitCheckout() {
  if (checkoutInFlight) return;
  if (pendingCheckout && !pendingCartRestored) {
    restorePendingCart(new Map(currentProducts.map((product) => [product.id, product])));
  }
  if (!pendingCheckout && (cart.size === 0 || !customersAvailable || !customerSelect.value)) {
    return;
  }

  if (!pendingCheckout) {
    try {
      createPendingCheckout();
    } catch {
      setCheckoutStatus("A secure retry key could not be created in this browser.", "error");
      return;
    }
  }

  checkoutInFlight = true;
  setCheckoutStatus("Submitting your order. The server will confirm its current prices and stock.");
  updateCheckoutControls();
  renderProducts(currentProducts);
  renderCart();

  try {
    const response = await fetch("/api/v1/orders", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "Idempotency-Key": pendingCheckout.key,
      },
      body: JSON.stringify(pendingCheckout.request),
      cache: "no-store",
      signal: AbortSignal.timeout(20_000),
    });

    if (response.status === 200 || response.status === 201) {
      let body;
      try {
        body = await response.json();
      } catch {
        setCheckoutStatus(
          `The server may have accepted the order, but its response could not be read. ${pendingRetryGuidance()}`,
          "error",
        );
        return;
      }

      if (!isUuid(body.id) || typeof body.status !== "string") {
        setCheckoutStatus(
          `The server may have accepted the order, but returned an unexpected response. ${pendingRetryGuidance()}`,
          "error",
        );
        return;
      }

      const orderId = body.id;
      clearPendingCheckout();
      cart.clear();
      setCheckoutStatus(`Order ${orderId} was submitted and is ${body.status}.`, "success");
      renderCart();
      return;
    }

    const message = await responseErrorMessage(response);
    if (response.status === 409) {
      setCheckoutStatus(
        `The saved retry key conflicts with a different request: ${message} The cart is locked to avoid creating a duplicate order.`,
        "error",
      );
      return;
    }

    if (response.status >= 500) {
      setCheckoutStatus(
        `The order result could not be confirmed: ${message} ${pendingRetryGuidance()}`,
        "error",
      );
      return;
    }

    clearPendingCheckout();
    setCheckoutStatus(`The order was rejected: ${message} Your cart is still here; correct it and try again.`, "error");
  } catch (error) {
    const reason = error?.name === "TimeoutError"
      ? "Checkout timed out before the result was confirmed."
      : "The API could not confirm the order.";
    setCheckoutStatus(`${reason} ${pendingRetryGuidance()}`, "error");
  } finally {
    checkoutInFlight = false;
    updateCheckoutControls();
    renderProducts(currentProducts);
    renderCart();
  }
}

customerSelect.addEventListener("change", () => {
  saveCustomerId(customerSelect.value);
  setCheckoutStatus("");
});
checkoutButton.addEventListener("click", submitCheckout);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refreshProducts();
});

if (pendingCheckout) {
  restorePendingCart(new Map());
  setCheckoutStatus(
    `A previous checkout may have reached the server. Retry the saved request to safely confirm it. ${pendingRetryGuidance()}`,
    "error",
  );
}
renderCart();
loadCustomers();
refreshProducts();
window.setInterval(refreshProducts, PRODUCT_POLL_INTERVAL_MS);
