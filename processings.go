package avro

import (
	"encoding/binary"
	"github.com/modern-go/reflect2"
	"golang.org/x/sync/singleflight"
	"sync"
	"unsafe"
)

type decoderOfTypeHandler func(cfg *frozenConfig, p *processing, schema Schema, typ reflect2.Type) ValDecoder
type encoderOfTypeHandler func(cfg *frozenConfig, p *processing, schema Schema, typ reflect2.Type) ValEncoder

type processing struct {
	key      []byte
	cfg      *frozenConfig
	decoders map[cacheKey]ValDecoder
	encoders map[cacheKey]ValEncoder
}

func (p *processing) addProcessingEncoderToCache(fingerprint [32]byte, rtype uintptr, enc ValEncoder) {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	p.encoders[key] = enc
}

func (p *processing) getProcessingEncoderFromCache(fingerprint [32]byte, rtype uintptr) ValEncoder {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if enc := p.cfg.getEncoderFromCache(fingerprint, rtype); enc != nil {
		return enc
	}
	if enc, ok := p.encoders[key]; ok {
		return enc
	}
	return nil
}

func (p *processing) addProcessingDecoderToCache(fingerprint [32]byte, rtype uintptr, dec ValDecoder) {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	p.decoders[key] = dec
}

func (p *processing) getProcessingDecoderFromCache(fingerprint [32]byte, rtype uintptr) ValDecoder {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if dec := p.cfg.getDecoderFromCache(fingerprint, rtype); dec != nil {
		return dec
	}
	if dec, ok := p.decoders[key]; ok {
		return dec
	}
	return nil
}

func (p *processing) clean() {
	for key := range p.encoders {
		delete(p.encoders, key)
	}
	for key := range p.decoders {
		delete(p.decoders, key)
	}
	p.cfg = nil
}

func newProcessingGroup() *processingGroup {
	return &processingGroup{
		group: new(singleflight.Group),
		pool:  new(sync.Pool),
	}
}

type processingGroup struct {
	group *singleflight.Group
	pool  *sync.Pool
}

func (ps *processingGroup) borrow() *processing {
	cached := ps.pool.Get()
	if cached != nil {
		return cached.(*processing)
	}
	return &processing{
		key:      make([]byte, 64),
		decoders: map[cacheKey]ValDecoder{},
		encoders: map[cacheKey]ValEncoder{},
	}
}

func (ps *processingGroup) processingDecoderOfType(cfg *frozenConfig, schema Schema, typ reflect2.Type, handler decoderOfTypeHandler) ValDecoder {
	p := ps.borrow()
	p.cfg = cfg
	fingerprint := schema.Fingerprint()
	rtype := typ.RType()
	copy(p.key[:32], fingerprint[:])
	binary.LittleEndian.PutUint64(p.key[32:], uint64(rtype))
	copy(p.key[:48], []byte{2})
	ptrType, isPtr := typ.(*reflect2.UnsafePtrType)
	if isPtr {
		typ = ptrType.Elem()
	}
	v, _, _ := ps.group.Do(*(*string)(unsafe.Pointer(&p.key)), func() (interface{}, error) {
		return handler(cfg, p, schema, typ), nil
	})
	dec := v.(ValDecoder)
	cfg.addDecoderToCache(schema.Fingerprint(), rtype, dec)
	ps.finish(p)
	return dec
}

func (ps *processingGroup) processingEncoderOfType(cfg *frozenConfig, schema Schema, typ reflect2.Type, handler encoderOfTypeHandler) ValEncoder {
	p := ps.borrow()
	p.cfg = cfg
	fingerprint := schema.Fingerprint()
	rtype := typ.RType()
	copy(p.key[:32], fingerprint[:])
	binary.LittleEndian.PutUint64(p.key[32:], uint64(rtype))
	copy(p.key[:48], []byte{1})
	v, _, _ := ps.group.Do(*(*string)(unsafe.Pointer(&p.key)), func() (interface{}, error) {
		return handler(cfg, p, schema, typ), nil
	})
	enc := v.(ValEncoder)
	if typ.LikePtr() {
		enc = &onePtrEncoder{enc}
	}
	cfg.addEncoderToCache(schema.Fingerprint(), rtype, enc)
	ps.finish(p)
	return enc
}

func (ps *processingGroup) finish(p *processing) {
	ps.group.Forget(*(*string)(unsafe.Pointer(&p.key)))
	p.clean()
	ps.pool.Put(p)
}
