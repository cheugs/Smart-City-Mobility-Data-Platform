/* Visual effects: custom cursor, card tilt, reveal on scroll, parallax titles,
   scroll-driven marquee, count-up numbers and the page transition.
   Everything is switched off when the system asks for reduced motion. */

const REDUCED = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const FINE_POINTER = window.matchMedia("(pointer: fine)").matches;

/* ---------------------------------------------------------------- cursor */
const Cursor = {
  x: window.innerWidth / 2, y: window.innerHeight / 2, rx: 0, ry: 0,
  init() {
    if (!FINE_POINTER || REDUCED) return;
    document.body.classList.add("has-cursor");
    this.dot = document.getElementById("cursor-dot");
    this.ring = document.getElementById("cursor-ring");
    this.label = document.getElementById("cursor-label");
    window.addEventListener("mousemove", (e) => { this.x = e.clientX; this.y = e.clientY; });
    // the ring grows over links, buttons and anything with data-cursor
    document.addEventListener("mouseover", (e) => {
      const target = e.target.closest("a, button, [data-cursor]");
      this.ring.classList.toggle("hover", !!target);
      this.label.textContent = target && target.dataset.cursor ? target.dataset.cursor : "";
    });
    const loop = () => {
      this.rx += (this.x - this.rx) * 0.18;     // the ring follows with a small delay
      this.ry += (this.y - this.ry) * 0.18;
      this.dot.style.transform = `translate(${this.x}px, ${this.y}px)`;
      this.ring.style.transform = `translate(${this.rx}px, ${this.ry}px)`;
      requestAnimationFrame(loop);
    };
    loop();
  },
};

/* ---------------------------------------------------------------- card tilt + light */
function enableTilt(root) {
  if (!FINE_POINTER || REDUCED) return;
  root.querySelectorAll(".card").forEach((card) => {
    card.addEventListener("mousemove", (e) => {
      const r = card.getBoundingClientRect();
      const px = (e.clientX - r.left) / r.width, py = (e.clientY - r.top) / r.height;
      card.style.setProperty("--mx", `${px * 100}%`);
      card.style.setProperty("--my", `${py * 100}%`);
      card.style.transform = `perspective(1100px) rotateX(${(0.5 - py) * 3}deg) rotateY(${(px - 0.5) * 3}deg)`;
    });
    card.addEventListener("mouseleave", () => { card.style.transform = ""; });
  });
}

/* ---------------------------------------------------------------- reveal on scroll */
function enableReveal(root) {
  const items = root.querySelectorAll("[data-reveal], .display");
  if (REDUCED || !("IntersectionObserver" in window)) { items.forEach((el) => el.classList.add("in")); return; }
  // A clip-path that hides the whole element also hides it from the observer,
  // so for "clip" reveals we watch the parent and reveal the child.
  const targets = new Map();
  const revealObserver = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        const el = targets.get(entry.target) || entry.target;
        el.classList.add("in");
        revealObserver.unobserve(entry.target);
      }
    });
  }, { threshold: 0.12, rootMargin: "0px 0px -6% 0px" });
  items.forEach((el) => {
    const watched = el.dataset.reveal === "clip" ? el.parentElement : el;
    targets.set(watched, el);
    revealObserver.observe(watched);
  });
}

/* split a title into words so each word can slide up from a mask */
function splitWords(el) {
  const words = el.textContent.trim().split(/\s+/);
  el.innerHTML = words.map((w, i) =>
    `<span class="word"><span style="transition-delay:${i * 90}ms">${w}</span></span>`).join("");
}

/* ---------------------------------------------------------------- scroll effects */
const Scroll = {
  lastY: window.scrollY, velocity: 0, offset: 0,
  init() {
    if (REDUCED) return;
    const loop = () => {
      const y = window.scrollY;
      this.velocity += ((y - this.lastY) - this.velocity) * 0.1;
      this.lastY = y;
      // hero title moves slower than the page (parallax)
      document.querySelectorAll(".hero .display").forEach((el) => {
        el.style.transform = `translateY(${y * 0.18}px)`;
      });
      // marquee: always moves a little, faster when the user scrolls
      document.querySelectorAll(".marquee-track").forEach((track) => {
        this.offset -= 0.6 + Math.abs(this.velocity) * 0.35;
        const half = track.scrollWidth / 2;
        if (half > 0 && -this.offset > half) this.offset += half;
        track.style.transform = `translateX(${this.offset}px)`;
      });
      requestAnimationFrame(loop);
    };
    loop();
  },
};

/* ---------------------------------------------------------------- count-up numbers */
function countUp(el, value, decimals = 0, format = (v) => v.toFixed(decimals)) {
  if (value === null || value === undefined || Number.isNaN(value)) { el.textContent = "—"; return; }
  if (REDUCED) { el.textContent = format(value); return; }
  const start = performance.now(), duration = 1300;
  const from = parseFloat(el.dataset.current || "0");
  el.dataset.current = value;
  const step = (now) => {
    const p = Math.min((now - start) / duration, 1);
    const eased = 1 - Math.pow(1 - p, 4);
    el.textContent = format(from + (value - from) * eased);
    if (p < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

/* ---------------------------------------------------------------- page transition */
function pageTransition(title, swap) {
  const wipe = document.getElementById("wipe");
  if (REDUCED) { swap(); return Promise.resolve(); }
  document.getElementById("wipe-title").textContent = title;
  return new Promise((resolve) => {
    wipe.className = "wipe";
    void wipe.offsetWidth;                // restart the animation
    wipe.classList.add("cover");
    setTimeout(() => {
      swap();
      window.scrollTo({ top: 0, behavior: "instant" });
      wipe.classList.remove("cover");
      wipe.classList.add("leave");
      setTimeout(() => { wipe.className = "wipe"; resolve(); }, 650);
    }, 600);
  });
}

/* ---------------------------------------------------------------- loader */
function runLoader() {
  const loader = document.getElementById("loader");
  const count = document.getElementById("loader-count");
  let n = 0;
  const timer = setInterval(() => {
    n = Math.min(100, n + Math.ceil(Math.random() * 18));
    count.textContent = String(n).padStart(2, "0");
    if (n >= 100) clearInterval(timer);
  }, 70);
  return () => { clearInterval(timer); count.textContent = "100"; setTimeout(() => loader.classList.add("done"), 250); };
}
