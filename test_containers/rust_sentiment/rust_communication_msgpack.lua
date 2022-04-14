sentence = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
msgpack = luajava.bindClass("org.msgpack.core.MessagePack")


-- Serialize all tokens as performance test
function serialize(inputCas,outputStream)
  packer = msgpack:newDefaultPacker(outputStream)
  packer:packString(inputCas:getDocumentText())

  local size = util:select(inputCas,sentence):size()
  if size == 0 then
    print(size)
    error("This annotator needs sentences, may be skipped in future versions")
  end

  packer:packArrayHeader(size*2)
  local result = util:select(inputCas,sentence):iterator()
  while result:hasNext() do
      local x = result:next()
	    packer:packInt(x:getBegin())
      packer:packInt(x:getEnd())
  end
  packer:close()
end

function deserialize(inputCas,inputStream)
  local result = util:select(inputCas,sentence):iterator()
  local unpack = msgpack:newDefaultUnpacker(inputStream)
  local size = unpack:unpackArrayHeader()
  while result:hasNext() do
      local x = result:next()
      local beg = x:getBegin())
      local endv = x:getEnd()
      local pol = unpack:unpackInt()
      local score =unpack:unpackFloat()
      print(x)
      print(beg)
      print(endv)
      print(score)
  end
end