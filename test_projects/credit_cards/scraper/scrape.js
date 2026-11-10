#!/usr/bin/env node
/*
 * Scrape one credit card company and write the transactions to a JSON file.
 *
 * Credentials are read from the SCRAPER_CREDENTIALS environment variable (JSON), never from
 * the command line, so they don't show up in process lists or Airflow logs.
 *
 *   SCRAPER_CREDENTIALS='{"username":"...","password":"..."}' \
 *     node scrape.js --company visaCal --start-date 2026-10-01 --out /tmp/cal.json
 *
 * Options:
 *   --company      visaCal | max | isracard
 *   --start-date   YYYY-MM-DD, earliest transaction date to fetch (the sites allow up to ~1 year back)
 *   --out          where to write the JSON result
 *   --profile-dir  optional Chromium profile folder, kept between runs so the site remembers this
 *                  "device" (reduces SMS/OTP prompts)
 *   --screenshot   optional path for a screenshot if the scrape fails (useful for debugging)
 *
 * Exit codes: 0 = success, 1 = bad usage, 2 = the site rejected the login / scrape failed.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { CompanyTypes, createScraper } = require('israeli-bank-scrapers');

const SUPPORTED = [CompanyTypes.visaCal, CompanyTypes.max, CompanyTypes.isracard];

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith('--')) throw new Error(`Unexpected argument: ${key}`);
    const value = argv[i + 1];
    if (value === undefined || value.startsWith('--')) throw new Error(`Missing value for ${key}`);
    args[key.slice(2)] = value;
    i += 1;
  }
  return args;
}

function usageError(message) {
  console.error(`Usage error: ${message}`);
  process.exit(1);
}

function prepareProfileDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
  // A crashed Chromium leaves lock files behind and then refuses to open the profile again.
  for (const lock of ['SingletonLock', 'SingletonSocket', 'SingletonCookie']) {
    fs.rmSync(path.join(dir, lock), { force: true });
  }
}

async function main() {
  let args;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (e) {
    return usageError(e.message);
  }

  const companyId = args.company;
  if (!SUPPORTED.includes(companyId)) return usageError(`--company must be one of ${SUPPORTED.join(', ')}`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(args['start-date'] || '')) return usageError('--start-date must be YYYY-MM-DD');
  if (!args.out) return usageError('--out is required');

  let credentials;
  try {
    credentials = JSON.parse(process.env.SCRAPER_CREDENTIALS || '');
  } catch (e) {
    return usageError('SCRAPER_CREDENTIALS env var must contain the credentials as JSON');
  }
  // Don't let the browser child process inherit the credentials.
  delete process.env.SCRAPER_CREDENTIALS;

  const browserArgs = ['--no-sandbox', '--disable-dev-shm-usage'];
  if (args['profile-dir']) {
    prepareProfileDir(args['profile-dir']);
    browserArgs.push(`--user-data-dir=${path.resolve(args['profile-dir'])}`);
  }
  if (args.screenshot) fs.mkdirSync(path.dirname(args.screenshot), { recursive: true });

  const scraper = createScraper({
    companyId,
    startDate: new Date(`${args['start-date']}T00:00:00`),
    combineInstallments: false, // one row per monthly installment, as it appears on the bill
    showBrowser: false,
    executablePath: process.env.CHROMIUM_PATH || undefined,
    args: browserArgs,
    defaultTimeout: 60000,
    storeFailureScreenShotPath: args.screenshot,
  });
  if (typeof scraper.onProgress === 'function') {
    scraper.onProgress((id, payload) => console.log(`[${id}] ${payload.type}`));
  }

  console.log(`Scraping ${companyId} from ${args['start-date']}...`);
  const result = await scraper.scrape(credentials);
  if (!result.success) {
    console.error(`Scrape failed: ${result.errorType}${result.errorMessage ? `: ${result.errorMessage}` : ''}`);
    process.exit(2);
  }

  const output = {
    company: companyId,
    startDate: args['start-date'],
    scrapedAt: new Date().toISOString(),
    accounts: result.accounts.map((account) => ({
      accountNumber: account.accountNumber,
      // rawTransaction is large and company-specific; we don't need it downstream.
      txns: account.txns.map(({ rawTransaction, ...txn }) => txn),
    })),
  };

  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  const tmp = `${args.out}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(output, null, 2));
  fs.renameSync(tmp, args.out); // atomic: readers never see a half-written file

  for (const account of output.accounts) {
    console.log(`Card ${account.accountNumber}: ${account.txns.length} transactions`);
  }
  console.log(`Wrote ${args.out}`);
}

main().catch((e) => {
  console.error(`Scraper crashed: ${e && e.stack ? e.stack : e}`);
  process.exit(2);
});
