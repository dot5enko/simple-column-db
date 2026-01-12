TEXT ·processBlockOfSingleByteDiffs(SB), $0-88
    // Register allocation:
    // R0: buf base pointer
    // R1: bufIdx (current)
    // R2: metaCount
    // R3: prev (accumulator)
    // R4: out base pointer
    // R5: idx (current)
    // R6: loop counter j
    // R7: temp for v
    // R8: temp for calculations
    // R9: temp for output pointer calculation
    
    // Load arguments
    MOVD buf+0(FP), R0       // buf base
    MOVD bufIdx+24(FP), R1   // bufIdx
    MOVD metaCount+32(FP), R2 // metaCount
    MOVD prev+40(FP), R3     // prev
    MOVD out+48(FP), R4      // out base
    MOVD idx+72(FP), R5      // idx
    
    // Initialize loop counter
    MOVD $0, R6              // j = 0
    
loop:
    // Check if j < 64 && idx < metaCount
    CMP $64, R6
    BGE end                  // if j >= 64, exit
    CMP R2, R5
    BGE end                  // if idx >= metaCount, exit
    
    // v := uint64(buf[bufIdx])
    ADD R1, R0, R8           // R8 = buf + bufIdx
    MOVBU (R8), R7           // R7 = v = buf[bufIdx] (zero-extended byte load)
    
    // bufIdx++
    ADD $1, R1, R1
    
    // Compute zigzag decode: (v >> 1) ^ (-(v & 1))
    AND $1, R7, R8           // R8 = v & 1
    NEG R8, R8               // R8 = -(v & 1) (two's complement negation)
    LSR $1, R7, R9           // R9 = v >> 1
    EOR R8, R9, R9           // R9 = (v >> 1) ^ (-(v & 1))
    
    // prev += decoded_value (treating R9 as signed)
    ADD R9, R3, R3           // prev += R9
    
    // out[idx] = prev
    MOVD R5, R8              // R8 = idx
    LSL $3, R8, R8           // R8 = idx * 8 (sizeof(uint64))
    ADD R8, R4, R8           // R8 = &out[idx]
    MOVD R3, (R8)            // out[idx] = prev
    
    // idx++
    ADD $1, R5, R5
    
    // j++
    ADD $1, R6, R6
    
    B loop
    
end:
    // Store return values
    MOVD R1, ret0+80(FP)     // return bufIdx
    MOVD R5, ret1+88(FP)     // return idx
    MOVD R3, ret2+96(FP)     // return prev
    RET

TEXT ·processBlockOfSingleByteDiffsOptimized(SB), $0-88
// Register allocation:
    // R0: buf base pointer
    // R1: bufIdx (current)
    // R2: metaCount
    // R3: prev (accumulator)
    // R4: out base pointer
    // R5: idx (current)
    // R6: loop counter j
    // R7: temp for v
    // R8: temp for calculations
    // R9: temp for output pointer calculation
    // R10: loop limit
    
    // Load arguments
    MOVD buf+0(FP), R0       // buf base
    MOVD bufIdx+24(FP), R1   // bufIdx
    MOVD metaCount+32(FP), R2 // metaCount
    MOVD prev+40(FP), R3     // prev
    MOVD out+48(FP), R4      // out base
    MOVD idx+72(FP), R5      // idx
    
    // Initialize loop counter
    MOVD $0, R6              // j = 0
    
    // Calculate loop limit: min(idx + 64, metaCount)
    ADD $64, R5, R10         // R10 = idx + 64
    CMP R2, R10              
    CSEL LT, R10, R2, R10    // R10 = min(idx + 64, metaCount)
    
loop:
    // Single check: idx < loop_limit
    CMP R10, R5
    BGE end                  // if idx >= limit, exit
    
    // v := uint64(buf[bufIdx])
    ADD R1, R0, R8           // R8 = buf + bufIdx
    MOVBU (R8), R7           // R7 = v = buf[bufIdx] (zero-extended byte load)
    
    // bufIdx++
    ADD $1, R1, R1
    
    // Compute zigzag decode: (v >> 1) ^ (-(v & 1))
    AND $1, R7, R8           // R8 = v & 1
    NEG R8, R8               // R8 = -(v & 1) (two's complement negation)
    LSR $1, R7, R9           // R9 = v >> 1
    EOR R8, R9, R9           // R9 = (v >> 1) ^ (-(v & 1))
    
    // prev += decoded_value (treating R9 as signed)
    ADD R9, R3, R3           // prev += R9
    
    // out[idx] = prev
    MOVD R5, R8              // R8 = idx
    LSL $3, R8, R8           // R8 = idx * 8 (sizeof(uint64))
    ADD R8, R4, R8           // R8 = &out[idx]
    MOVD R3, (R8)            // out[idx] = prev
    
    // idx++
    ADD $1, R5, R5
    
    B loop
    
end:
    // Store return values
    MOVD R1, ret0+80(FP)     // return bufIdx
    MOVD R5, ret1+88(FP)     // return idx
    MOVD R3, ret2+96(FP)     // return prev
    RET
    