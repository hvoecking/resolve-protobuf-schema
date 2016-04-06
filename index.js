var schema = require('protocol-buffers-schema')
var fs = require('fs')
var path = require('path')

function mapBy(array, attribute, fn) {
  var grouped = {}
  array.map(function(item) {
    var key = item[attribute]
    grouped[key] = (grouped[key] || []).concat([item])
  })
  var results = []
  Object.keys(grouped).forEach(function(key) {
    results.push(fn(key, grouped[key], grouped))
  })
  return results
}

var merge = function(a, b) {
  var messages = a.messages.concat(b.messages)
  a.messages = mapBy(messages, 'fullName', function(fullName, messageGroup) {
    var fields = []
    messageGroup.map(function(message) {
      fields = fields.concat(message.fields)
    })

    var merged = messageGroup[0]
    merged.fields = mapBy(fields, 'name', function(name, fieldGroup) {
      return fieldGroup[0]
    })
    return merged
  })

  a.enums = a.enums.concat(b.enums)

  return a
}

function mergeSchemas(schemas) {
  var sch = schemas.reduce(merge)
  delete sch.package
  delete sch.imports
  return sch
}

function collectMessages(schema, ns, f) {
  schema.messages.forEach(function(msg) {
    var fullName = msg.fullName || (ns ? [ns, msg.name].join('.') : msg.name)
    f(msg, fullName)
    collectMessages(msg, fullName, f)
  })
  var extends_ = schema.extends || []
  extends_.forEach(function(ext) {
    f(ext.message, ns)
  })
}

function collectExtends(schema, ns, f) {
  (schema.extends || []).forEach(function(ext) {
    f(ext, ns)
  })
  collectMessages(schema, ns, function(msg, ns) {
    collectExtends(msg, ns, f)
  })
}

function collectEnums(schema, ns, f) {
  schema.enums.forEach(function(en) {
    var shouldPrefix = ns && !en.name.match(/\./)
    var fullName = shouldPrefix ? [ns, en.name].join('.') : en.name
    f(en, fullName)
  })
  collectMessages(schema, ns, function(msg, ns) {
    collectEnums(msg, ns, f)
  })
}

function collectFields(schema, ns, f) {
  collectMessages(schema, ns, function(message, ns) {
    message.fields.forEach(function(field) {
      f(field, ns)
    })
  })
}

function collectIntoArray(schema, collector) {
  var values = []
  collector(schema, schema.package, function(value) {
    values.push(value)
  })
  return values
}

function qualifyFieldTypes(schema) {
  var messages = collectIntoArray(schema, collectMessages)
  var enums = collectIntoArray(schema, collectEnums)
  var types = messages.concat(enums)
  collectFields(schema, schema.package, function(field, ns) {
    var type = field.type
    var refCandidates = getCandidates(ns, type)
    refCandidates.some(function(refCandidate) {
      return types.some(function(type) {
        if (type.fullName === refCandidate) {
          field.type = refCandidate
          return true
        }
        return false
      })
    })
  })
}

function qualifyMessages(schema) {
  collectMessages(schema, schema.package, function(msg, fullName) {
    msg.fullName = fullName
  })
  collectEnums(schema, schema.package, function(en, fullName) {
    en.fullName = fullName
  })
  schema.messages.forEach(function(msg) {
    msg.name = msg.fullName
  })
  schema.enums.forEach(function(en) {
    en.name = en.fullName
  })
}

function getCandidates(ns, ref) {
  var results = [ref]

  var index
  var candidatePrefix
  while (true) {
    index = ns.indexOf('.', index + 1)
    if (index === -1) {
        break
    }
    candidateNs = ns.slice(0, index)
    results.unshift(candidatePrefix + '.' + ref)
  }

  results.unshift(ns + '.' + ref)
  return results
}

function extendMessage(ext, msg) {
  ext.message.fields.forEach(function (field) {
    if (!msg.extensions || field.tag < msg.extensions.from || field.tag > msg.extensions.to) {
      throw new Error(msg.name + ' does not declare ' + field.tag +
                      ' as an extension number')
    }
    msg.fields.push(field)
  })
}

function propagateExtends(schemas) {
  schemas.reduceRight(function(messages, extSchema) {
    var messages = collectIntoArray(extSchema, collectMessages).concat(messages)

    collectExtends(extSchema, extSchema.package, function(ext, ns) {
      var refCandidates = getCandidates(ns, ext.name)
      var matchingMessage
      refCandidates.some(function(refCandidate) {
        return messages.some(function(message) {
          if (message.fullName === refCandidate) {
            matchingMessage = message
            return true
          }
          return false
        })
      })
      if (matchingMessage) {
        extendMessage(ext, matchingMessage)
      } else {
        throw new Error(ext.name + ' extend references unknown message')
      }
    })

    return messages
  }, [])
  return schemas
}

var readSync = function(filename, protoPaths) {
  protoPaths = protoPaths || []
  protoPaths = protoPaths.concat(path.dirname(filename))
  if (!/\.proto$/i.test(filename) && !fs.existsSync(filename)) filename += '.proto'

  var sch = schema(fs.readFileSync(filename, 'utf-8'))
  var imports = [].concat(sch.imports || [])
  var schemas = [sch]

  imports.forEach(function(i) {
    var resolved = null
    protoPaths.every(function(protoPath) {
      resolved = path.resolve(protoPath, i)
      return !fs.existsSync(resolved)
    })
    schemas = schemas.concat(readSync(resolved, protoPaths))
  })

  return schemas
}

function resolveImport(importFile, protoPaths, cb) {
  var paths = [].concat(protoPaths || [])
  var resolvedFile = null

  var resolveLoop = function(resolved) {
    if (resolved) return cb(resolvedFile)
    if (!paths.length) return cb(null)

    resolvedFile = path.resolve(paths.shift(), importFile)
    fs.exists(resolvedFile, resolveLoop)
  }
  resolveLoop()
}

var read = function(filename, protoPaths, cb) {
  protoPaths = protoPaths || []
  protoPaths = protoPaths.concat([path.dirname(filename)])

  fs.exists(filename, function(exists) {
    if (!exists && !/\.proto$/i.test(filename)) filename += '.proto'

    fs.readFile(filename, 'utf-8', function(err, proto) {
      if (err) return cb(err)

      var sch = schema(proto)
      var imports = [].concat(sch.imports || [])
      var schemas = [sch]

      var loop = function() {
        if (!imports.length) return cb(null, schemas)

        resolveImport(imports.shift(), protoPaths, function(resolvedFile) {
          read(resolvedFile, protoPaths, function(err, ch) {
            if (err) return cb(err)
            schemas = schemas.concat(ch)
            loop()
          })
        })
      }

      loop()
    })
  })
}

function readAndMerge(filename /*, protoPaths, cb */) {
  var args = [].slice.call(arguments)
  var protoPaths = args.slice(1, -1)[0]
  var cb = args.slice(-1)[0]

  read(filename, protoPaths, function(err, schemas) {
    if (err) return cb(err)
    schemas.forEach(qualifyMessages)
    schemas.forEach(qualifyFieldTypes)
    propagateExtends(schemas)
    var sch = mergeSchemas(schemas)
    cb(null, sch)
  })
}

function readAndMergeSync(filename, protoPaths) {
  var schemas = readSync(filename, protoPaths)
  schemas.forEach(qualifyMessages)
  schemas.forEach(qualifyFieldTypes)
  propagateExtends(schemas)
  var sch = mergeSchemas(schemas)
  return sch
}

module.exports = readAndMerge
module.exports.sync = readAndMergeSync
