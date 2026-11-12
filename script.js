const slides = Array.from(document.querySelectorAll('.slide'));
const slideCounter = document.getElementById('slideCounter');
const progressBar = document.getElementById('progressBar');
const prevBtn = document.getElementById('prevBtn');
const nextBtn = document.getElementById('nextBtn');
const overview = document.getElementById('overview');
const overviewGrid = document.getElementById('overviewGrid');
const overviewBtn = document.getElementById('overviewBtn');
const closeOverviewBtn = document.getElementById('closeOverviewBtn');
const fullscreenBtn = document.getElementById('fullscreenBtn');
const printBtn = document.getElementById('printBtn');

let currentIndex = 0;
let touchStartX = null;

slides.forEach((slide, index) => {
  const title = slide.querySelector('h1, h2')?.textContent.trim() || `Diapositive ${index + 1}`;
  const section = slide.querySelector('.section-tag')?.textContent.trim() || 'Introduction';
  const card = document.createElement('button');
  card.className = 'overview-card';
  card.type = 'button';
  card.innerHTML = `<span class="overview-number">${String(index + 1).padStart(2, '0')}</span><span class="overview-section">${section}</span><strong>${title}</strong>`;
  card.addEventListener('click', () => {
    showSlide(index);
    setOverview(false);
  });
  overviewGrid.appendChild(card);
});

function updateSlideUI() {
  slides.forEach((slide, index) => {
    slide.classList.toggle('active', index === currentIndex);
    slide.setAttribute('aria-hidden', String(index !== currentIndex));
  });

  slideCounter.textContent = `${currentIndex + 1} / ${slides.length}`;
  const progress = ((currentIndex + 1) / slides.length) * 100;
  progressBar.style.width = `${progress}%`;
  overviewGrid.querySelectorAll('.overview-card').forEach((card, index) => {
    card.classList.toggle('selected', index === currentIndex);
  });

  prevBtn.disabled = currentIndex === 0;
  prevBtn.style.opacity = currentIndex === 0 ? '0.45' : '1';
  nextBtn.textContent = currentIndex === slides.length - 1 ? '✓' : '→';
  nextBtn.setAttribute('aria-label', currentIndex === slides.length - 1 ? 'Recommencer la présentation' : 'Diapositive suivante');
}

function showSlide(index) {
  currentIndex = Math.max(0, Math.min(index, slides.length - 1));
  updateSlideUI();
  history.replaceState(null, '', `#slide-${currentIndex + 1}`);
}

function setOverview(open) {
  overview.hidden = !open;
  document.body.classList.toggle('overview-open', open);
  if (open) {
    closeOverviewBtn.focus();
  } else {
    overviewBtn.focus();
  }
}

async function toggleFullscreen() {
  try {
    if (!document.fullscreenElement) {
      await document.querySelector('.deck-shell').requestFullscreen();
    } else {
      await document.exitFullscreen();
    }
  } catch (error) {
    console.error('Impossible de modifier le mode plein écran :', error);
  }
}

function navigateBy(offset) {
  if (overview.hidden) {
    showSlide(currentIndex + offset);
  }
}

prevBtn.addEventListener('click', () => {
  showSlide(currentIndex - 1);
});

nextBtn.addEventListener('click', () => {
  if (currentIndex === slides.length - 1) {
    showSlide(0);
    return;
  }
  showSlide(currentIndex + 1);
});

overviewBtn.addEventListener('click', () => setOverview(true));
closeOverviewBtn.addEventListener('click', () => setOverview(false));
fullscreenBtn.addEventListener('click', toggleFullscreen);
printBtn.addEventListener('click', () => window.print());

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && !overview.hidden) {
    setOverview(false);
    return;
  }

  if (event.target instanceof HTMLElement && ['INPUT', 'TEXTAREA', 'BUTTON'].includes(event.target.tagName) && event.key !== 'Escape') {
    return;
  }

  if (event.key === 'ArrowRight' || event.key === 'PageDown' || event.key === ' ') {
    event.preventDefault();
    navigateBy(1);
  }

  if (event.key === 'ArrowLeft' || event.key === 'PageUp') {
    event.preventDefault();
    navigateBy(-1);
  }

  if (event.key.toLowerCase() === 'f') {
    toggleFullscreen();
  }

  if (event.key.toLowerCase() === 'o') {
    setOverview(overview.hidden);
  }
});

document.addEventListener('fullscreenchange', () => {
  const isFullscreen = Boolean(document.fullscreenElement);
  fullscreenBtn.textContent = isFullscreen ? '⛶' : '⛶';
  fullscreenBtn.title = isFullscreen ? 'Quitter le plein écran' : 'Plein écran';
  document.querySelector('.deck-shell').classList.toggle('is-fullscreen', isFullscreen);
});

document.addEventListener('touchstart', (event) => {
  touchStartX = event.changedTouches[0].clientX;
}, { passive: true });

document.addEventListener('touchend', (event) => {
  if (touchStartX === null || !overview.hidden) return;
  const distance = event.changedTouches[0].clientX - touchStartX;
  if (Math.abs(distance) > 55) navigateBy(distance < 0 ? 1 : -1);
  touchStartX = null;
}, { passive: true });

const initialSlide = Number(window.location.hash.match(/^#slide-(\d+)$/)?.[1]) - 1;
if (Number.isInteger(initialSlide) && initialSlide >= 0 && initialSlide < slides.length) {
  currentIndex = initialSlide;
}

updateSlideUI();
