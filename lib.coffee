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

exports.createGzipFromParts = (parts) ->
	out = CombinedStream.create()
	# write the header
	out.append(GZIP_HEADER)
	# write all middle parts
	out.append(stream) for { stream } in parts
	# write ending DEFLATE part
	out.append(DEFLATE_END)
	# write CRC
	# 🤖-expanation: The CRC32 shift matrix M has period T = 2^32-1 (primitive polynomial),
	# so M^n = M^(n mod T). Reduce each part's len mod T before passing to
	# crc32_combine_multi, which accepts len as a 32-bit integer.
	# Special case: if n mod T = 0 (and n > 0), use T instead of 0 to avoid
	# the len=0 early-return in crc32_combine that would ignore crc2.
	CRC32_PERIOD_NUMBER = 0xffffffff; # T = 2^32-1
	normalizedParts = parts.map (p) ->
		# crc32_combine(crc1, crc2, n) receives len as a 32-bit integer, so any len >= 2^32 must be reduced here.
		if p.len <= CRC32_PERIOD_NUMBER
			return p
		len = p.len % CRC32_PERIOD_NUMBER
		if len == 0
			len = CRC32_PERIOD_NUMBER
		return { crc: p.crc, len: len }
	out.append(crcUtils.crc32_combine_multi(normalizedParts).combinedCrc32)
	# write the ISIZE length, modulo 2^32 per RFC 1952 section 2.3.1
	# https://www.rfc-editor.org/info/rfc1952/#page-8:~:text=original%20(uncompressed)%20input-,data%20modulo%202%5E32
	len = Buffer.alloc(4)
	isize = parts.reduce(((sum, p) -> sum + p.len), 0) % 0x100000000
	len.writeUInt32LE(isize, 0)
	out.append(len)
	# calculate compressed size. Add 10 byte header, 2 byte DEFLATE ending block, 8 byte footer
	out.zLen = parts.reduce(((sum, p) -> sum + p.zLen), 0) + 20
	# return stream
	return out
