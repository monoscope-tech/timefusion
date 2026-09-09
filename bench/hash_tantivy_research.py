#!/usr/bin/env python3
"""Exact per-element Tantivy indexing and index-only histograms on synthetic data.

Requires Python bindings with Searcher.aggregate. Writes only a temporary index.
"""
import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

import tantivy


def visibility():
    b=tantivy.SchemaBuilder()
    b.add_text_field('key',tokenizer_name='raw',index_option='basic')
    b.add_text_field('hashes',tokenizer_name='raw',index_option='basic')
    b.add_integer_field('ts',fast=True,indexed=True)
    index=tantivy.Index(b.build())
    w=index.writer(num_threads=1)
    # Full event identity must be encoded here in a real integration.
    w.add_document(tantivy.Document(key='event-a',hashes=['a','a','b'],ts=10))
    w.commit();index.reload()
    q=index.parse_query('hashes:a')
    agg={'h':{'histogram':{'field':'ts','interval':10,'min_doc_count':1}}}
    count=lambda s:sum(x['doc_count'] for x in s.aggregate(q,agg)['h']['buckets'])
    old_snapshot=index.searcher()
    assert count(old_snapshot)==1, 'duplicate array element counted twice'
    # Atomic index commit removes the old document and inserts its replacement.
    w.delete_documents('key','event-a')
    w.add_document(tantivy.Document(key='event-a',hashes=['b'],ts=10))
    w.commit();index.reload()
    assert count(index.searcher())==0, 'removed tag still matches'
    assert count(old_snapshot)==1, 'old searcher lost snapshot isolation'
    w.delete_documents('key','event-a')
    w.add_document(tantivy.Document(key='event-a',hashes=['a','b'],ts=10))
    w.commit();index.reload()
    assert count(index.searcher())==1, 'replacement counted in wrong event-time bucket'
    w.delete_documents('key','event-a')
    w.commit();index.reload()
    assert count(index.searcher())==0, 'deleted event still counted'
    return {'version':tantivy.__version__,'checks_passed':[
        'duplicate element counts once','tag removal','old searcher snapshot',
        'tag addition at original event time','event deletion'],
        'scope':'Single Tantivy index commit. Does not prove atomic visibility across TimeFusion files, Delta and memory.'}


def run(n, output):
    b = tantivy.SchemaBuilder()
    b.add_text_field("hashes", tokenizer_name="raw", index_option="basic")
    b.add_integer_field("ts", fast=True, indexed=True)
    schema = b.build()
    result = {"version": tantivy.__version__, "rows": n, "queries": []}
    with tempfile.TemporaryDirectory(prefix="tf-hash-tantivy-") as path:
        index = tantivy.Index(schema, path=path)
        writer = index.writer(heap_size=128_000_000, num_threads=1)
        start = time.perf_counter()
        for i in range(1, n+1):
            tags = ["endpoint:common" if i%10 else "endpoint:other"]
            for divisor, tag in [(1000,"err:rare"),(100,"err:medium"),(200,"err:overlap")]:
                if i%divisor == 0:
                    tags.append(tag)
            # Integer microseconds from an arbitrary epoch. Same 30-day span.
            writer.add_document(tantivy.Document(hashes=tags, ts=(i-1)*30*86400*1_000_000//n))
        writer.commit()
        writer.wait_merging_threads()
        result["build_seconds"] = time.perf_counter()-start
        index.reload()
        result["index_bytes"] = sum(p.stat().st_size for p in Path(path).iterdir() if p.is_file())
        searcher = index.searcher()
        cases = {"absent": 'hashes:"err:absent"', "rare": 'hashes:"err:rare"',
                 "medium": 'hashes:"err:medium"', "common": 'hashes:"endpoint:common"',
                 "overlap": '(hashes:"err:medium" OR hashes:"err:overlap")'}
        for days in (3,7,30):
            lo=(30-days)*86400*1_000_000
            hi=30*86400*1_000_000-1
            for name, pred in cases.items():
                query=index.parse_query(f"({pred}) AND ts:[{lo} TO {hi}]")
                for width in (3600,1020):
                    agg={"chart":{"histogram":{"field":"ts", "interval":width*1_000_000, "min_doc_count":1}}}
                    samples=[]
                    for _ in range(5):
                        t=time.perf_counter()
                        response=searcher.aggregate(query, agg)
                        samples.append((time.perf_counter()-t)*1000)
                    # Independent expected per-bucket distribution; scans only
                    # the synthetic matching ordinal progression for each tag.
                    step={"absent":n+1,"rare":1000,"medium":100,"common":1,"overlap":100}[name]
                    expected={}
                    for i in range(step,n+1,step):
                        if name=="common" and i%10==0:
                            continue
                        ts=(i-1)*30*86400*1_000_000//n
                        if lo<=ts<=hi:
                            bucket=ts//(width*1_000_000)*(width*1_000_000)
                            expected[bucket]=expected.get(bucket,0)+1
                    actual={int(x['key']):x['doc_count'] for x in response['chart']['buckets']}
                    assert actual==expected,(days,name,width)
                    result['queries'].append({'days':days,'case':name,'width_seconds':width,
                        'ms':samples,'median_ms':statistics.median(samples),'count':sum(actual.values()),
                        'buckets':len(actual),'all_buckets_match':True})
            print("finished days",days,flush=True)
    output.write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rows',type=int,default=1_000_000)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--semantics-only',action='store_true')
    args=p.parse_args()
    if args.semantics_only:
        args.output.write_text(json.dumps(visibility(),indent=2)+'\n')
    else:
        run(args.rows,args.output)
