var tape = require('tape')
var schema = require('../')

var test = function(name, fn) {
  tape(name, function(t) {
    fn(t, schema)
  })
  tape(name+' sync', function(t) {
    fn(t, function(name /*, protoPaths, cb */) {
      var args = [].slice.call(arguments)
      var protoPaths = args.slice(1, -1)[0]
      var cb = args.slice(-1)[0]

      cb(null, schema.sync(name, protoPaths))
    })
  })
}

test('c', function(t, schema) {
  schema(__dirname+'/c.proto', function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch.messages.length, 1)
    schema(__dirname+'/c', function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 1)
      t.end()
    })
  })
})

test('b imports c', function(t, schema) {
  schema(__dirname+'/b.proto', function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch.messages.length, 2)
    schema(__dirname+'/b', function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 2)
      t.end()
    })
  })
})

test('a imports b imports c', function(t, schema) {
  schema(__dirname+'/a.proto', function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch.messages.length, 3)
    schema(__dirname+'/a', function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 3)
      t.end()
    })
  })
})

test('G references f.F', function(t, schema) {
  schema(__dirname+'/g.proto', function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch, require('./g.json'))
    schema(__dirname+'/g', function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 3)
      t.end()
    })
  })
})

test('d imports e imports f from external proto paths', function(t, schema) {
  var protoPaths = [__dirname+'/include_e', __dirname+'/include_f']
  schema(__dirname+'/d.proto', protoPaths, function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch.messages.length, 3)
    schema(__dirname+'/d', protoPaths, function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 3)
      t.end()
    })
  })
})

test('h', function(t, schema) {
  schema(__dirname+'/h.proto', function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch.messages.length, 1)
    schema(__dirname+'/h', function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 1)
      t.end()
    })
  })
})

test('i extends h', function(t, schema) {
  schema(__dirname+'/i.proto', function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch.messages.length, 2)
    schema(__dirname+'/i', function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 2)
      t.end()
    })
  })
})

test('j extends h', function(t, schema) {
  schema(__dirname+'/j.proto', function(err, sch) {
    t.notOk(err, 'no err')
    t.same(sch.messages.length, 2)
    schema(__dirname+'/j', function(err, sch) {
      t.notOk(err, 'no err')
      t.same(sch.messages.length, 2)
      t.end()
    })
  })
})

test('k has field of type i and j', function(t, schema) {
  function check(sch) {
    var messageH = sch.messages.filter(function(message) {
     return message.fullName === 'H'
    })
    t.same(messageH.length, 1)
    var hMessages = messageH[0].fields.map(function(message) {
      return message.name
    })
    t.notSame(hMessages.indexOf('i'), -1)
    t.notSame(hMessages.indexOf('j'), -1)
  }

  schema(__dirname+'/k.proto', function(err, sch) {
    t.notOk(err, 'no err')
    check(sch)
    schema(__dirname+'/k', function(err, sch) {
      t.notOk(err, 'no err')
      check(sch)
      t.end()
    })
  })
})
