import { Readable as NodeReadable } from "node:stream";
import { pipeline as streamPipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const UPSTREAM_BASE = (process.env.WEBSITE_ADDRESS || "").replace(/\/$/, "");

const HEADER_BLOCKLIST = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

function buildForwardHeaders(incomingReq) {
  /** @type {Record<string, string>} */
  const outboundHeaders = {};
  let clientIpAddress = null;

  for (const originalKey of Object.keys(incomingReq.headers)) {
    const normalizedKey = originalKey.toLowerCase();
    const rawValue = incomingReq.headers[originalKey];

    // فیلتر هدرهای ممنوع
    if (HEADER_BLOCKLIST.has(normalizedKey)) continue;
    if (normalizedKey.startsWith("x-vercel-")) continue;

    // تعیین IP کلاینت
    if (normalizedKey === "x-real-ip") {
      clientIpAddress = rawValue;
      continue;
    }
    if (normalizedKey === "x-forwarded-for") {
      if (!clientIpAddress) clientIpAddress = rawValue;
      continue;
    }

    // تبدیل آرایه هدر به رشته
    outboundHeaders[normalizedKey] = Array.isArray(rawValue)
      ? rawValue.join(", ")
      : rawValue;
  }

  if (clientIpAddress) {
    outboundHeaders["x-forwarded-for"] = clientIpAddress;
  }

  return outboundHeaders;
}

function shouldHaveBody(method) {
  const upper = (method || "").toUpperCase();
  return upper !== "GET" && upper !== "HEAD";
}

async function relayResponse(upstreamResp, outgoingRes) {
  outgoingRes.statusCode = upstreamResp.status;

  // ست‌کردن هدرهای پاسخ
  for (const [headerName, headerValue] of upstreamResp.headers) {
    if (headerName.toLowerCase() === "transfer-encoding") continue;
    try {
      outgoingRes.setHeader(headerName, headerValue);
    } catch {
      // اگر هدر ولید نبود، سایلنت فیل شود
    }
  }

  // استریم بدنه
  const bodyStream = upstreamResp.body;
  if (!bodyStream) {
    outgoingRes.end();
    return;
  }

  await streamPipeline(NodeReadable.fromWeb(bodyStream), outgoingRes);
}

export default async function proxyHandler(incomingReq, outgoingRes) {
  if (!UPSTREAM_BASE) {
    outgoingRes.statusCode = 500;
    return outgoingRes.end("Misconfigured: WEBSITE_ADDRESS is not set");
  }

  const targetUrl = UPSTREAM_BASE + incomingReq.url;

  try {
    const forwardHeaders = buildForwardHeaders(incomingReq);
    const method = incomingReq.method;
    const hasBody = shouldHaveBody(method);

    /** @type {RequestInit & { duplex?: "half" }} */
    const fetchOptions = {
      method,
      headers: forwardHeaders,
      redirect: "manual",
    };

    if (hasBody) {
      fetchOptions.body = NodeReadable.toWeb(incomingReq);
      fetchOptions.duplex = "half";
    }

    const upstreamResponse = await fetch(targetUrl, fetchOptions);

    await relayResponse(upstreamResponse, outgoingRes);
  } catch (error) {
    console.error("relay error:", error);
    if (!outgoingRes.headersSent) {
      outgoingRes.statusCode = 502;
      outgoingRes.end("Bad Gateway: Tunnel Failed");
    }
  }
}
