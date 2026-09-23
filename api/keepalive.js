// General Supabase Keep-Alive for Vercel
//
// Save as:
// api/keepalive.js
//
// This performs one tiny READ request against test_table
// in each Supabase project.
//
// It does NOT insert, update, or delete anything.

const PROJECTS = [
  {
    name: 'Money Tracker',
    url: 'https://fvzrphhacxchvicekluj.supabase.co',
    key: 'sb_publishable_6i0q8k_jhf2yPKgqB1yNrA_cq4ER4Wt',
    table: 'test_table',
  },
  {
    name: 'Website ni Mama',
    url: 'https://carodzdpubzulokuvefl.supabase.co',
    key: 'sb_publishable_cnRdXcQn3gTlYYVlRe8heg_P6tucIyP',
    table: 'test_table',
  },
  {
    name: 'Pokemon Tracker',
    url: 'https://dzbpzlkpjbascrjozcih.supabase.co',
    key: 'sb_publishable_AXT6yLPWDsSJ43uf14KMIQ_tlEoOt4r',
    table: 'test_table',
  },
  {
    name: 'Restaurant Tracker',
    url: 'https://bosjigsuxudsaqqaorhi.supabase.co',
    key: 'sb_publishable_YsQwlmyFBauRTDJtF9aKoA_B-sW9ZDj',
    table: 'test_table',
  },
];

async function pingProject(project) {
  const url =
    `${project.url}/rest/v1/${encodeURIComponent(project.table)}` +
    `?select=*&limit=1`;

  try {
    const result = await fetch(url, {
      method: 'GET',

      headers: {
        apikey: project.key,
        Authorization: `Bearer ${project.key}`,
        Accept: 'application/json',
        'Cache-Control': 'no-cache, no-store, max-age=0',
      },

      cache: 'no-store',
    });

    const body = await result.text();

    return {
      name: project.name,
      ok: result.ok,
      status: result.status,
      table: project.table,
      error: result.ok
        ? null
        : body.slice(0, 300),
    };

  } catch (error) {
    return {
      name: project.name,
      ok: false,
      status: 0,
      table: project.table,
      error:
        error instanceof Error
          ? error.message
          : String(error),
    };
  }
}

export default async function handler(request, response) {

  // Only allow GET
  if (
    request.method &&
    request.method !== 'GET'
  ) {
    response.setHeader('Allow', 'GET');

    return response.status(405).json({
      ok: false,
      error: 'Method not allowed. Use GET.',
    });
  }

  try {

    // Ping all Supabase projects
    const results = await Promise.all(
      PROJECTS.map(pingProject)
    );

    const failed = results.filter(
      (item) => !item.ok
    );

    // Prevent cached responses
    response.setHeader(
      'Cache-Control',
      'no-store, no-cache, must-revalidate, proxy-revalidate'
    );

    // Something failed
    if (failed.length > 0) {
      return response.status(502).json({
        ok: false,

        message:
          `${failed.length} of ${results.length} ` +
          `Supabase keep-alive checks failed.`,

        results,

        checkedAt:
          new Date().toISOString(),
      });
    }

    // Everything worked
    return response.status(200).json({
      ok: true,

      message:
        `All ${results.length} Supabase projects ` +
        `were pinged successfully.`,

      results,

      checkedAt:
        new Date().toISOString(),
    });

  } catch (error) {

    return response.status(500).json({
      ok: false,

      error:
        error instanceof Error
          ? error.message
          : String(error),

      checkedAt:
        new Date().toISOString(),
    });
  }
}