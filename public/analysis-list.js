/**
 * 試合ログ分析一覧ページ
 * analysis-index.json と analysis-folders/*.json からフォルダ別ページを生成する。
 */

function formatSec(sec) {
  const s = Math.max(0, Math.round(Number(sec) || 0));
  const m = Math.floor(s / 60);
  const ss = String(s % 60).padStart(2, '0');
  return `${m}:${ss}`;
}

function resultBadge(score, yName, bName) {
  if (score.yellow > score.blue)   return { cls: 'yellow-win', text: `${yName} 勝利` };
  if (score.blue   > score.yellow) return { cls: 'blue-win',   text: `${bName} 勝利` };
  return { cls: 'draw', text: '引き分け' };
}

function buildMatchCard(meta) {
  const { id, filename, teams, final_score, duration_sec, gdrive_url, team_stats } = meta;
  const yName = teams?.yellow || 'Yellow';
  const bName = teams?.blue   || 'Blue';
  const score = final_score || { yellow: 0, blue: 0 };
  const result = resultBadge(score, yName, bName);

  const yWinner = result.cls === 'yellow-win';
  const bWinner = result.cls === 'blue-win';

  const dlHtml = gdrive_url
    ? `<a href="${gdrive_url}" class="match-download-link" target="_blank" rel="noopener" title="ログファイルをダウンロード (Google Drive)" onclick="event.stopPropagation()">&#x1F4E5;</a>`
    : '';

  let teamStatsHtml = '';
  if (team_stats) {
    const ys = team_stats.yellow;
    const bs = team_stats.blue;
    const yPTotal = (ys.placement_succeeded ?? 0) + (ys.placement_failed ?? 0);
    const bPTotal = (bs.placement_succeeded ?? 0) + (bs.placement_failed ?? 0);
    const yRate = ys.placement_success_rate;
    const bRate = bs.placement_success_rate;
    const yRateStr = yRate !== null && yRate !== undefined ? yRate + '%' : '–';
    const bRateStr = bRate !== null && bRate !== undefined ? bRate + '%' : '–';
    teamStatsHtml = `
      <div class="match-team-stats">
        <span class="match-team-stat-y">${ys.shots ?? 0}本</span>
        <span class="match-team-stat-label">ショット</span>
        <span class="match-team-stat-b">${bs.shots ?? 0}本</span>
      </div>
      <div class="match-team-stats">
        <span class="match-team-stat-y">${yRateStr}${yPTotal > 0 ? ` (${ys.placement_succeeded}/${yPTotal})` : ''}</span>
        <span class="match-team-stat-label">BP成功率</span>
        <span class="match-team-stat-b">${bRateStr}${bPTotal > 0 ? ` (${bs.placement_succeeded}/${bPTotal})` : ''}</span>
      </div>
    `;
  }

  const a = document.createElement('a');
  a.href = `./analysis.html?id=${encodeURIComponent(id)}`;
  a.className = 'match-card';
  a.innerHTML = `
    <div class="match-scoreboard">
      <div class="match-team-col match-team-col--yellow ${yWinner ? 'winner' : ''}">
        <span class="match-team-name">${yName}</span>
        <span class="match-score-num match-score-yellow">${score.yellow}</span>
      </div>
      <div class="match-score-sep">–</div>
      <div class="match-team-col match-team-col--blue ${bWinner ? 'winner' : ''}">
        <span class="match-score-num match-score-blue">${score.blue}</span>
        <span class="match-team-name">${bName}</span>
      </div>
    </div>
    <div class="match-meta-row">
      <span class="match-result-badge ${result.cls}">${result.text}</span>
      <span class="match-duration-badge">⏱ ${formatSec(duration_sec)}</span>
    </div>
    ${teamStatsHtml}
    <div class="match-card-filename">${filename || id}${dlHtml}</div>
  `;
  return a;
}

function formatMin(sec) {
  return ((Number(sec) || 0) / 60).toFixed(1);
}

function summarizeMatches(matches) {
  const totalGoals = matches.reduce(
    (s, m) => s + (m.final_score?.yellow || 0) + (m.final_score?.blue || 0), 0
  );
  const avgDurationSec = matches.length > 0
    ? matches.reduce((s, m) => s + (m.duration_sec || 0), 0) / matches.length
    : 0;

  return {
    match_count: matches.length,
    total_goals: totalGoals,
    avg_duration_sec: avgDurationSec,
  };
}

function summarizeFolders(folders) {
  const matchCount = folders.reduce((s, f) => s + (f.match_count || 0), 0);
  const totalGoals = folders.reduce((s, f) => s + (f.total_goals || 0), 0);
  const totalDurationSec = folders.reduce(
    (s, f) => s + (f.avg_duration_sec || 0) * (f.match_count || 0), 0
  );

  return {
    match_count: matchCount,
    total_goals: totalGoals,
    avg_duration_sec: matchCount > 0 ? totalDurationSec / matchCount : 0,
  };
}

function updateSummary(summary) {
  document.getElementById('total-matches').textContent = String(summary.match_count || 0);
  document.getElementById('avg-duration').textContent = formatMin(summary.avg_duration_sec);
  document.getElementById('total-goals').textContent = String(summary.total_goals || 0);
}

function folderUrl(folderId) {
  return `./analysis-list.html?folder=${encodeURIComponent(folderId)}`;
}

function groupMatchesByFolder(matches) {
  const folders = [];
  const groupMap = new Map();

  for (const meta of matches) {
    const folderId = meta.gdrive_folder_id || 'root';
    if (!groupMap.has(folderId)) {
      const folder = {
        id: folderId,
        name: meta.gdrive_folder || '未分類',
        path: meta.gdrive_folder_path || '',
        matches: [],
      };
      groupMap.set(folderId, folder);
      folders.push(folder);
    }
    groupMap.get(folderId).matches.push(meta);
  }

  return folders.map(folder => ({ ...folder, ...summarizeMatches(folder.matches) }));
}

function buildFolderCard(folder) {
  const a = document.createElement('a');
  a.href = folderUrl(folder.id);
  a.className = 'folder-card';

  const head = document.createElement('div');
  head.className = 'folder-card-head';

  const title = document.createElement('h3');
  title.className = 'folder-card-title';
  title.textContent = folder.name || '未分類';

  const count = document.createElement('span');
  count.className = 'folder-card-count';
  count.textContent = `${folder.match_count || 0}試合`;

  head.appendChild(title);
  head.appendChild(count);
  a.appendChild(head);

  if (folder.path && folder.path !== folder.name) {
    const path = document.createElement('div');
    path.className = 'folder-card-path';
    path.textContent = folder.path;
    a.appendChild(path);
  }

  const stats = document.createElement('div');
  stats.className = 'folder-card-stats';
  stats.innerHTML = `
    <span>平均 ${formatMin(folder.avg_duration_sec)}分</span>
    <span>${folder.total_goals || 0}ゴール</span>
  `;
  a.appendChild(stats);

  return a;
}

function buildStandingsTable(matches) {
  const teamMap = new Map();

  for (const m of matches) {
    const gy = m.final_score?.yellow ?? 0;
    const gb = m.final_score?.blue   ?? 0;
    const yName = m.teams?.yellow || 'Yellow';
    const bName = m.teams?.blue   || 'Blue';

    for (const name of [yName, bName]) {
      if (!teamMap.has(name)) teamMap.set(name, { w: 0, d: 0, l: 0, gf: 0, ga: 0 });
    }

    const yt = teamMap.get(yName);
    const bt = teamMap.get(bName);
    yt.gf += gy; yt.ga += gb;
    bt.gf += gb; bt.ga += gy;

    if (gy > gb)      { yt.w++; bt.l++; }
    else if (gb > gy) { bt.w++; yt.l++; }
    else              { yt.d++; bt.d++; }
  }

  const rows = [...teamMap.entries()].sort((a, b) => {
    const ap = a[1].w * 3 + a[1].d;
    const bp = b[1].w * 3 + b[1].d;
    if (bp !== ap) return bp - ap;
    return (b[1].gf - b[1].ga) - (a[1].gf - a[1].ga);
  });

  const wrap = document.createElement('div');
  wrap.className = 'standings-wrap';
  const rowsHtml = rows.map(([name, s]) => {
    const gd = s.gf - s.ga;
    const gdCls = gd > 0 ? 'standings-pos' : gd < 0 ? 'standings-neg' : '';
    const gdStr = gd > 0 ? `+${gd}` : String(gd);
    return `<tr>
      <td class="standings-team">${name}</td>
      <td>${s.w}</td><td>${s.d}</td><td>${s.l}</td>
      <td>${s.gf}</td><td>${s.ga}</td>
      <td class="${gdCls}">${gdStr}</td>
      <td class="standings-pts">${s.w * 3 + s.d}</td>
    </tr>`;
  }).join('');

  wrap.innerHTML = `
    <h3 class="standings-title">チーム成績</h3>
    <div class="an-table-wrap" style="overflow-x:auto">
      <table class="standings-table">
        <thead><tr>
          <th>チーム</th>
          <th>勝</th><th>分</th><th>負</th>
          <th>得点</th><th>失点</th><th>得失差</th><th>勝点</th>
        </tr></thead>
        <tbody>${rowsHtml}</tbody>
      </table>
    </div>
  `;
  return wrap;
}

function buildMatchSection(matches) {
  const section = document.createElement('section');
  section.className = 'match-folder-section';

  const grid = document.createElement('div');
  grid.className = 'match-card-grid';
  for (const meta of matches) {
    grid.appendChild(buildMatchCard(meta));
  }

  section.appendChild(grid);
  return section;
}

function setListHeader(title, description) {
  document.getElementById('list-title').textContent = title;
  document.getElementById('list-description').textContent = description;
}

function clearContent() {
  const content = document.getElementById('analysis-list-content');
  content.innerHTML = '';
  return content;
}

function renderFolderIndex(indexJson) {
  const matches = indexJson.matches || [];
  const folders = (indexJson.folders && indexJson.folders.length > 0)
    ? indexJson.folders
    : groupMatchesByFolder(matches);
  const summary = matches.length > 0 ? summarizeMatches(matches) : summarizeFolders(folders);
  const content = clearContent();

  document.title = '試合ログ分析一覧 – ibis-ssl';
  setListHeader('フォルダ一覧', `${folders.length}フォルダ / ${summary.match_count || 0}試合`);
  updateSummary(summary);

  if (folders.length === 0) {
    content.innerHTML = '<p class="no-data-msg">試合データがありません。CI を実行してデータを生成してください。</p>';
    return;
  }

  const grid = document.createElement('div');
  grid.className = 'folder-card-grid';
  for (const folder of folders) {
    grid.appendChild(buildFolderCard(folder));
  }
  content.appendChild(grid);
}

function renderFolderPage(folderJson) {
  const folder = folderJson.folder || { name: '未分類' };
  const matches = folderJson.matches || [];
  const summary = summarizeMatches(matches);
  const content = clearContent();

  document.title = `${folder.name || '未分類'} – 試合ログ分析`;
  setListHeader(folder.name || '未分類', `${summary.match_count || 0}試合 / 平均 ${formatMin(summary.avg_duration_sec)}分`);
  updateSummary(summary);

  const toolbar = document.createElement('div');
  toolbar.className = 'match-folder-toolbar';

  const backLink = document.createElement('a');
  backLink.href = './analysis-list.html';
  backLink.className = 'folder-back-link';
  backLink.textContent = 'フォルダ一覧';
  toolbar.appendChild(backLink);
  content.appendChild(toolbar);

  if (matches.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'no-data-msg';
    empty.textContent = '試合データがありません。';
    content.appendChild(empty);
    return;
  }

  const uniqueTeams = new Set();
  for (const m of matches) {
    if (m.teams?.yellow) uniqueTeams.add(m.teams.yellow);
    if (m.teams?.blue)   uniqueTeams.add(m.teams.blue);
  }
  if (uniqueTeams.size >= 2) {
    content.appendChild(buildStandingsTable(matches));
  }

  content.appendChild(buildMatchSection(matches));
}

function fetchJson(url) {
  return fetch(url).then(r => {
    if (!r.ok) throw new Error(`${url}: ${r.status} ${r.statusText}`);
    return r.json();
  });
}

async function loadFolderPage(folderId) {
  try {
    return await fetchJson(`./analysis-folders/${encodeURIComponent(folderId)}.json`);
  } catch (folderErr) {
    const indexJson = await fetchJson('./analysis-index.json');
    const folders = indexJson.folders || [];
    const folder = folders.find(f => f.id === folderId) || { id: folderId, name: '未分類' };
    const matches = (indexJson.matches || []).filter(m => (m.gdrive_folder_id || 'root') === folderId);
    if (matches.length === 0) throw folderErr;
    return { folder, matches };
  }
}

async function init() {
  const params = new URLSearchParams(window.location.search);
  const folderId = params.get('folder');

  if (folderId) {
    renderFolderPage(await loadFolderPage(folderId));
  } else {
    renderFolderIndex(await fetchJson('./analysis-index.json'));
  }

  document.getElementById('loading-msg').style.display = 'none';
}

init().catch(err => {
  document.getElementById('loading-msg').textContent =
    `分析データの読み込みに失敗しました: ${err.message}`;
});
