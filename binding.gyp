{
    'targets': [{
        'target_name': 'nlmdb'
      , 'dependencies': [
            '<(module_root_dir)/deps/liblmdb.gyp:liblmdb'
        ]
      , 'include_dirs' : [
          "<!(node -e \"require('nan')\")"
        ]
      , 'sources': [
            'src/nlmdb.cc'
          , 'src/database.cc'
          , 'src/database_async.cc'
          , 'src/batch.cc'
          , 'src/batch_async.cc'
          , 'src/iterator.cc'
          , 'src/iterator_async.cc'
        ]
    }]
}
