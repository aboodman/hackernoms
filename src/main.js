// @flow

import firebase from 'firebase';
import async from 'async';
import { newStruct, Map, DatasetSpec, Dataset } from '@attic/noms';

main().catch(ex => {
  console.error('\nError:', ex);
  if (ex.stack) {
    console.error(ex.stack);
  }
  process.exit(1);
});

let maxItem, lastItem;
let caughtUp = false;
let done = false;

let iter = {};
iter[Symbol.iterator] = function () {
  return {
    next: function () {
      if (caughtUp || done) return { done: true };
      lastItem++;
      if (lastItem == maxItem) caughtUp = true;
      return { value: "/v0/item/" + lastItem, done: false }
    }
  };
};

var all = Promise.resolve(new Map());

function newItem(v) {
  console.log(v['id']);
  const t = v['type']; // XXX Noms can't deal with a field named 'type'...
  delete v['type'];
  delete v['kids']; // XXX Ignore the array for now.
  const n = newStruct(t, v);

  all = all.then(a => {
    return a.set(v['id'], n);
  });
}

async function main(): Promise<void> {
  // Initialize the app with no authentication
  firebase.initializeApp({
    databaseURL: "https://hacker-news.firebaseio.com"
  });

  // The app only has access to public data as defined in the Security Rules
  const fdb = firebase.database();

  var v = await fdb.ref("/v0/maxitem").once("value");

  maxItem = v.val();
  lastItem = 0;

  const process = () => {
    async.eachLimit(iter, 100, (n, done) => {
      const onVal = v => {
        const value = v.val();
        if (value !== null) {
          newItem(value);
          done();
        } else {
          // For unknown reasons we see nulls even for items known to be
          // valid. If we hit this condition, wait a second between retries.
          setTimeout(function () {
            fdb.ref(n).once("value", onVal);
          }, 1000);
        }
      };
      fdb.ref(n).once("value", onVal);
    });
  };

  // Subscribe to the maxitem.
  fdb.ref("/v0/maxitem").on("value", v => {
    maxItem = v.val();
    if (caughtUp) {
      caughtUp = false;
      process();
    }
  });

  process();

  const spec = DatasetSpec.parse('http://localhost:8000::hn');
  let ds = spec.dataset();

  let last = null;

  const maybeCommit = async () => {
    console.log('checking whether commit necessary...')
    let a = await all;
    if (last !== a) {
      console.log('...yup')
      try {
        ds = await ds.commit(a);
      } catch (ex) {
        process.exitCode = 1;
        console.log(ex);
        done = true;
      }
      last = all;
    } else {
      console.log('...nawp')
    }
    scheduleCommit();
  }

  const scheduleCommit = () => {
    setTimeout(maybeCommit, 1 * 1000);
  };

  scheduleCommit();
}
