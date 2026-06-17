crcUtils = require '@balena/node-crc-utils'
CombinedStream = require 'combined-stream'
{ DeflateCRC32Stream } = require 'crc32-stream'

# gzip header
exports.GZIP_HEADER = GZIP_HEADER = Buffer.from([ 0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff ])

# DEFLATE ending block
exports.DEFLATE_END = DEFLATE_END = Buffer.from([ 0x03, 0x00 ])
exports.DEFLATE_END_LENGTH = DEFLATE_END_LENGTH = DEFLATE_END.length

# Use the logic briefly described here by the author of zlib library:
# http://stackoverflow.com/questions/14744692/concatenate-multiple-zlib-compressed-data-streams-into-a-single-stream-efficient#comment51865187_14744792
# to generate deflate streams that can be concatenated into a gzip stream
class DeflatePartStream extends DeflateCRC32Stream
	constructor: ->
		super(arguments...)
		@buf = Buffer.alloc(0)
	push: (chunk) ->
		if chunk isnt null
			if chunk.length >= DEFLATE_END_LENGTH
				# got another large enough chunk, previous chunk is safe to send
				super(@buf)
				@buf = chunk
			else
				@buf = Buffer.concat([@buf, chunk])
		else
			# got null signalling end of stream
			# inspect last chunk for DEFLATE_END marker and remove it
			if @buf.length >= DEFLATE_END_LENGTH and @buf[-DEFLATE_END_LENGTH..].equals(DEFLATE_END)
				@buf = @buf[...-DEFLATE_END_LENGTH]
			super(@buf)
			super(null)
	end: ->
		@flush =>
			super()
	metadata: ->
		crc: @digest().readUInt32BE(0)
		len: @size()
		zLen: @size(true)

exports.createDeflatePart = ->
	return new DeflatePartStream()

exports.getGzipSizeFromParts = (parts) ->
	# calculate compressed size. Add 10 byte header, 2 byte DEFLATE ending block, 8 byte footer
	zLen = parts.map((p) -> p.zLen).reduce((a, b) -> a + b) + 20
	return zLen

exports.createGzipFromParts = (parts) ->
	out = CombinedStream.create()
	# write the header
	out.append(GZIP_HEADER)
	# write all middle parts
	out.append(stream) for { stream } in parts
	# write ending DEFLATE part
	out.append(DEFLATE_END)
	# write CRC
	out.append(crcUtils.crc32_combine_multi(parts).combinedCrc32)
	# write the ISIZE length, modulo 2^32 per RFC 1952 section 2.3.1
	# https://www.rfc-editor.org/info/rfc1952/#page-8:~:text=original%20(uncompressed)%20input-,data%20modulo%202%5E32
	len = Buffer.alloc(4)
	isize = parts.map((p) -> p.len).reduce((a, b) -> a + b) % 0x100000000
	len.writeUInt32LE(isize, 0)
	out.append(len)
	# calculate compressed size.
	out.zLen = exports.getGzipSizeFromParts(parts)
	# return stream
	return out
