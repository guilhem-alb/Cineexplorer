const movie_cards = document.getElementsByClassName("movie-card");

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function delayed_fetch(card) {
    const movie_id = card.dataset.imdbId
    const res = await fetch("https://api.imdbapi.dev/titles/" + movie_id);
    const data = await res.json();
    const img_url = data.primaryImage?.url;
    if(img_url) {
        card.querySelector("img").setAttribute("src", img_url);
    }
}

async function loadAllMovieCards() {
    for (const card of movie_cards) {
        await delayed_fetch(card);
        await sleep(1); // avoid 429 error from the api by slowing requests
    }
}

loadAllMovieCards();