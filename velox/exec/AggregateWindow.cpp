/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/exec/AggregateWindow.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

namespace {

// A generic way to compute any aggregation used as a window function.
// Creates an Aggregate function object for the window function invocation.
// At each row, computes the aggregation across all rows from the frameStart
// to frameEnd boundaries at that row using singleGroup.
class AggregateWindowFunction : public exec::WindowFunction {
 public:
  AggregateWindowFunction(
      const std::string& name,
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      bool ignoreNulls,
      velox::memory::MemoryPool* pool,
      HashStringAllocator* stringAllocator,
      const core::QueryConfig& config)
      : WindowFunction(resultType, pool, stringAllocator) {
    VELOX_USER_CHECK(
        !ignoreNulls, "Aggregate window functions do not support IGNORE NULLS");
    argTypes_.reserve(args.size());
    argIndices_.reserve(args.size());
    argVectors_.reserve(args.size());
    for (const auto& arg : args) {
      argTypes_.push_back(arg.type);
      if (arg.constantValue) {
        argIndices_.push_back(kConstantChannel);
        argVectors_.push_back(arg.constantValue);
      } else {
        VELOX_CHECK(arg.index.has_value());
        argIndices_.push_back(arg.index.value());
        argVectors_.push_back(BaseVector::create(arg.type, 0, pool_));
      }
    }
    // Create an Aggregate function object to do result computation. Window
    // function usage only requires single group aggregation for calculating
    // the function value for each row.
    aggregate_ = exec::Aggregate::create(
        name,
        core::AggregationNode::Step::kSingle,
        argTypes_,
        resultType,
        config);
    aggregate_->setAllocator(stringAllocator_);

    // Aggregate initialization.
    // Row layout is:
    //  - null flags - one bit per aggregate.
    //  - uint32_t row size,
    //  - fixed-width accumulators - one per aggregate
    //
    // Here we always make space for a row size since we only have one
    // row and no RowContainer. We also have a single aggregate here, so there
    // is only one null bit and one initialized bit.
    static const int32_t kAccumulatorFlagsOffset = 0;
    static const int32_t kRowSizeOffset = bits::nbytes(1);
    singleGroupRowSize_ = kRowSizeOffset + sizeof(int32_t);
    // Accumulator offset must be aligned by their alignment size.
    singleGroupRowSize_ = bits::roundUp(
        singleGroupRowSize_, aggregate_->accumulatorAlignmentSize());
    aggregate_->setOffsets(
        singleGroupRowSize_,
        exec::RowContainer::nullByte(kAccumulatorFlagsOffset),
        exec::RowContainer::nullMask(kAccumulatorFlagsOffset),
        exec::RowContainer::initializedByte(kAccumulatorFlagsOffset),
        exec::RowContainer::initializedMask(kAccumulatorFlagsOffset),
        /* needed for out of line allocations */ kRowSizeOffset);
    singleGroupRowSize_ += aggregate_->accumulatorFixedWidthSize();

    // Construct the single row in the MemoryPool.
    singleGroupRowBufferPtr_ =
        AlignedBuffer::allocate<char>(singleGroupRowSize_, pool_);
    rawSingleGroupRow_ = singleGroupRowBufferPtr_->asMutable<char>();

    // Constructing a vector of a single result value used for copying from
    // the aggregate to the final result.
    aggregateResultVector_ = BaseVector::create(resultType, 1, pool_);

    supportsRetract_ = aggregate_->supportsRetract();

    computeDefaultAggregateValue(resultType);
  }

  ~AggregateWindowFunction() {
    // Needed to delete any out-of-line storage for the accumulator in the
    // group row.
    if (aggregateInitialized_) {
      std::vector<char*> singleGroupRowVector = {rawSingleGroupRow_};
      aggregate_->destroy(folly::Range(singleGroupRowVector.data(), 1));
    }
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;

    previousFrameMetadata_.reset();
    slidingState_.reset();
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    if (handleAllEmptyFrames(validRows, resultOffset, result)) {
      return;
    }

    auto rawFrameStarts = frameStarts->as<vector_size_t>();
    auto rawFrameEnds = frameEnds->as<vector_size_t>();

    FrameMetadata frameMetadata =
        analyzeFrameValues(validRows, rawFrameStarts, rawFrameEnds);

    if (frameMetadata.incrementalAggregation) {
      vector_size_t startRow;
      if (frameMetadata.usePreviousAggregate) {
        // If incremental aggregation can be resumed from the previous block,
        // then the argument vectors also can be populated from the previous
        // frameEnd to the current frameEnd. Only the new values are
        // required for computing aggregates.
        startRow = previousFrameMetadata_->lastRow + 1;
      } else {
        startRow = frameMetadata.firstRow;

        // This is the start of a new incremental aggregation. So the
        // aggregate_ function object should be initialized.
        auto singleGroup = std::vector<vector_size_t>{0};
        aggregate_->destroy(folly::Range<char**>(&rawSingleGroupRow_, 1));
        aggregate_->initializeNewGroups(&rawSingleGroupRow_, singleGroup);
        aggregateInitialized_ = true;
      }

      // An incremental block invalidates any sliding state from a prior block.
      slidingState_.reset();

      fillArgVectors(startRow, frameMetadata.lastRow);
      incrementalAggregation(
          validRows,
          startRow,
          frameMetadata.lastRow,
          rawFrameEnds,
          resultOffset,
          result);
    } else if (frameMetadata.slidingAggregation) {
      fillArgVectors(frameMetadata.firstRow, frameMetadata.lastRow);
      slidingAggregation(
          validRows,
          frameMetadata.firstRow,
          frameMetadata.lastRow,
          rawFrameStarts,
          rawFrameEnds,
          frameMetadata.useSlidingState,
          resultOffset,
          result);
    } else {
      // The accumulator state from any prior sliding block is unreachable
      // from a non-sliding/non-incremental block; drop it so we never try to
      // resume from inconsistent state.
      slidingState_.reset();

      fillArgVectors(frameMetadata.firstRow, frameMetadata.lastRow);
      simpleAggregation(
          validRows,
          frameMetadata.firstRow,
          frameMetadata.lastRow,
          rawFrameStarts,
          rawFrameEnds,
          resultOffset,
          result);
    }
    previousFrameMetadata_ = frameMetadata;
  }

 private:
  struct FrameMetadata {
    // Min frame start row required for aggregation.
    vector_size_t firstRow;

    // Max frame end required for the aggregation.
    vector_size_t lastRow;

    // If all the rows in the block have the same start row, and the
    // end frame rows are non-decreasing, then the aggregation can be done
    // incrementally. With incremental aggregation new frame rows are
    // accumulated over the previous result to obtain the new result.
    bool incrementalAggregation;

    // Resume incremental aggregation from the prior block.
    bool usePreviousAggregate;

    // If both frame starts and frame ends are non-decreasing across this
    // block (and the underlying aggregate supports retract), the aggregation
    // can be performed in linear time by retracting rows that leave the
    // frame and adding rows that enter it. Mutually exclusive with
    // incrementalAggregation.
    bool slidingAggregation;

    // Resume the sliding accumulator state from the prior block.
    bool useSlidingState;
  };

  // Tracks the accumulator's contents across apply() calls within a single
  // partition for the sliding-window path. Coordinates are absolute within
  // the partition (i.e., the same space as rawFrameStarts/rawFrameEnds).
  struct SlidingState {
    // Inclusive start of the rows currently in the accumulator.
    vector_size_t accumStartAbs;

    // Exclusive end of the rows currently in the accumulator.
    vector_size_t accumEndAbs;

    // Number of non-null contributions currently in the accumulator. When
    // this drops to zero we substitute the empty-frame result rather than
    // extracting from the accumulator, since the underlying null flag is
    // not restored by retract.
    int64_t nonNullCount;
  };

  bool handleAllEmptyFrames(
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    if (!validRows.hasSelections()) {
      setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
      return true;
    }
    return false;
  }

  // Computes the least frameStart row and the max frameEnds row
  // indices for the valid frames of this output block. These indices are used
  // as bounds when reading input parameter vectors for aggregation.
  // This method expects to have at least 1 valid frame in the block.
  // Blocks with all empty frames are handled before this point.
  FrameMetadata analyzeFrameValues(
      const SelectivityVector& validRows,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds) {
    VELOX_DCHECK(validRows.hasSelections());

    // Use first valid frame row for the initialization.
    auto firstValidRow = validRows.begin();
    vector_size_t firstRow = rawFrameStarts[firstValidRow];
    vector_size_t fixedFrameStartRow = firstRow;
    vector_size_t lastRow = rawFrameEnds[firstValidRow];
    vector_size_t prevFrameStart = firstRow;
    vector_size_t prevFrameEnd = lastRow;

    bool fixedStart = true;
    bool nonDecreasingStart = true;
    bool nonDecreasingEnd = true;
    validRows.applyToSelected([&](auto i) {
      firstRow = std::min(firstRow, rawFrameStarts[i]);
      lastRow = std::max(lastRow, rawFrameEnds[i]);

      fixedStart &= (rawFrameStarts[i] == fixedFrameStartRow);
      nonDecreasingStart &= (rawFrameStarts[i] >= prevFrameStart);
      nonDecreasingEnd &= (rawFrameEnds[i] >= prevFrameEnd);
      prevFrameStart = rawFrameStarts[i];
      prevFrameEnd = rawFrameEnds[i];
    });

    // Incremental aggregation handles expanding frames (fixed start,
    // non-decreasing end) and works for all aggregates. Sliding aggregation
    // handles frames where the start also advances; it requires the
    // aggregate to support retract.
    bool incrementalAggregation = fixedStart && nonDecreasingEnd;
    bool slidingAggregation = !incrementalAggregation && nonDecreasingStart &&
        nonDecreasingEnd && supportsRetract_;

    bool usePreviousAggregate = false;
    bool useSlidingState = false;
    if (previousFrameMetadata_.has_value()) {
      const auto& previousFrame = previousFrameMetadata_.value();
      // Incremental aggregation continues between blocks if :
      // i) Their starting firstRow values are the same.
      // ii) The nonDecreasing frameEnd property is also applicable between the
      // lastRow of the first block and the first row of the current block.
      if (incrementalAggregation && previousFrame.incrementalAggregation &&
          previousFrame.firstRow == firstRow &&
          previousFrame.lastRow <= rawFrameEnds[firstValidRow]) {
        usePreviousAggregate = true;
      } else if (
          slidingAggregation && slidingState_.has_value() &&
          slidingState_->accumStartAbs <= rawFrameStarts[firstValidRow] &&
          slidingState_->accumEndAbs <= rawFrameEnds[firstValidRow] + 1) {
        // The saved accumulator is a predecessor of this block's first
        // frame: we can retract from its start and add toward its end.
        useSlidingState = true;
        // Extend firstRow to include the saved accumulator's leading rows
        // so their argument values are present for retraction.
        firstRow = std::min(firstRow, slidingState_->accumStartAbs);
      }
    }

    return {
        firstRow,
        lastRow,
        incrementalAggregation,
        usePreviousAggregate,
        slidingAggregation,
        useSlidingState};
  }

  void fillArgVectors(vector_size_t firstRow, vector_size_t lastRow) {
    vector_size_t numFrameRows = lastRow + 1 - firstRow;
    for (int i = 0; i < argIndices_.size(); i++) {
      // Without the following call to `prepareForReuse`, if the type of
      // `argVectors_[i]` is VARCHAR, then the string buffers will accumulate
      // across calculations. As a result, memory consumption will increase over
      // time. So we call `prepareForReuse` to clear string buffers timely.
      argVectors_[i]->prepareForReuse();
      argVectors_[i]->resize(numFrameRows);
      // Only non-constant field argument vectors need to be populated. The
      // constant vectors are correctly set during aggregate initialization
      // itself.
      if (argIndices_[i] != kConstantChannel) {
        partition_->extractColumn(
            argIndices_[i], firstRow, numFrameRows, 0, argVectors_[i]);
      }
    }
  }

  void computeAggregate(
      SelectivityVector rows,
      vector_size_t startFrame,
      vector_size_t endFrame) {
    rows.clearAll();
    rows.setValidRange(startFrame, endFrame, true);
    rows.updateBounds();

    BaseVector::prepareForReuse(aggregateResultVector_, 1);

    aggregate_->addSingleGroupRawInput(
        rawSingleGroupRow_, rows, argVectors_, false);
    aggregate_->extractValues(&rawSingleGroupRow_, 1, &aggregateResultVector_);
  }

  void incrementalAggregation(
      const SelectivityVector& validRows,
      vector_size_t startFrame,
      vector_size_t endFrame,
      const vector_size_t* rawFrameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    SelectivityVector rows;
    rows.resize(endFrame + 1 - startFrame);

    auto prevFrameEnd = 0;
    // This is a simple optimization for frames that have a fixed startFrame
    // and increasing frameEnd values. In that case, we can
    // incrementally aggregate over the new rows seen in the frame between
    // the previous and current row.
    validRows.applyToSelected([&](auto i) {
      auto currentFrameEnd = rawFrameEnds[i] - startFrame + 1;
      if (currentFrameEnd > prevFrameEnd) {
        computeAggregate(rows, prevFrameEnd, currentFrameEnd);
      }

      result->copy(aggregateResultVector_.get(), resultOffset + i, 0, 1);
      prevFrameEnd = currentFrameEnd;
    });

    // Set null values for empty (non valid) frames in the output block.
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  // Evaluates the aggregation over a sliding window. Both frame starts and
  // frame ends are non-decreasing across this block, and the underlying
  // aggregate supports retract, so each row's frame can be obtained from the
  // previous row's by retracting rows that left the leading edge and adding
  // rows that entered on the trailing edge — linear in the partition size
  // rather than quadratic in the frame width.
  void slidingAggregation(
      const SelectivityVector& validRows,
      vector_size_t firstRow,
      vector_size_t lastRow,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds,
      bool useSlidingState,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    const vector_size_t numFrameRows = lastRow + 1 - firstRow;

    // Precompute the per-row non-null status of the first argument over the
    // local window. The Window operator tracks how many non-null
    // contributions remain in the accumulator because the underlying null
    // flag is not restored on retract; without that, the aggregate would
    // return 0 instead of NULL once every row falls out of the frame.
    // For argument-less aggregates (e.g. count(*)) every row counts.
    std::vector<bool> rowIsNonNull;
    if (!argVectors_.empty()) {
      rowIsNonNull.resize(numFrameRows);
      DecodedVector decoded(*argVectors_[0]);
      for (vector_size_t i = 0; i < numFrameRows; ++i) {
        rowIsNonNull[i] = !decoded.isNullAt(i);
      }
    }
    auto countContributions = [&](vector_size_t localStart,
                                  vector_size_t localEndExcl) -> int64_t {
      if (argVectors_.empty()) {
        return localEndExcl - localStart;
      }
      int64_t n = 0;
      for (vector_size_t i = localStart; i < localEndExcl; ++i) {
        if (rowIsNonNull[i]) {
          ++n;
        }
      }
      return n;
    };

    SelectivityVector rows;
    rows.resize(numFrameRows);

    // [accumStart, accumEnd) tracks the rows currently in the accumulator,
    // in local (argVectors_) coordinates.
    vector_size_t accumStart;
    vector_size_t accumEnd;
    int64_t nonNullCount;

    if (useSlidingState) {
      accumStart = slidingState_->accumStartAbs - firstRow;
      accumEnd = slidingState_->accumEndAbs - firstRow;
      nonNullCount = slidingState_->nonNullCount;
    } else {
      static const auto kSingleGroup = std::vector<vector_size_t>{0};
      aggregate_->destroy(folly::Range<char**>(&rawSingleGroupRow_, 1));
      aggregate_->initializeNewGroups(&rawSingleGroupRow_, kSingleGroup);
      aggregateInitialized_ = true;
      accumStart = rawFrameStarts[validRows.begin()] - firstRow;
      accumEnd = accumStart;
      nonNullCount = 0;
    }

    validRows.applyToSelected([&](auto i) {
      const auto localStart = rawFrameStarts[i] - firstRow;
      const auto localEndExcl = rawFrameEnds[i] - firstRow + 1;

      // Retract leading-edge rows that have fallen out of the frame. With
      // non-decreasing starts and ends across the partition, the retract
      // range is everything in the accumulator below localStart.
      const auto retractEnd = std::min<vector_size_t>(localStart, accumEnd);
      if (retractEnd > accumStart) {
        rows.clearAll();
        rows.setValidRange(accumStart, retractEnd, true);
        rows.updateBounds();
        aggregate_->removeSingleGroupRawInput(
            rawSingleGroupRow_, rows, argVectors_);
        nonNullCount -= countContributions(accumStart, retractEnd);
      }

      // Add trailing-edge rows newly in the frame. If localStart > accumEnd
      // (a gap because the frame jumped past the previous range), the gap
      // rows are correctly excluded — only [localStart, localEndExcl) is
      // added.
      const auto addStart = std::max<vector_size_t>(localStart, accumEnd);
      if (localEndExcl > addStart) {
        rows.clearAll();
        rows.setValidRange(addStart, localEndExcl, true);
        rows.updateBounds();
        aggregate_->addSingleGroupRawInput(
            rawSingleGroupRow_, rows, argVectors_, false);
        nonNullCount += countContributions(addStart, localEndExcl);
      }

      accumStart = localStart;
      accumEnd = localEndExcl;

      if (nonNullCount == 0) {
        result->copy(emptyResult_.get(), resultOffset + i, 0, 1);
      } else {
        BaseVector::prepareForReuse(aggregateResultVector_, 1);
        aggregate_->extractValues(
            &rawSingleGroupRow_, 1, &aggregateResultVector_);
        result->copy(aggregateResultVector_.get(), resultOffset + i, 0, 1);
      }
    });

    slidingState_ = SlidingState{
        accumStart + firstRow,
        accumEnd + firstRow,
        nonNullCount,
    };

    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  void simpleAggregation(
      const SelectivityVector& validRows,
      vector_size_t minFrame,
      vector_size_t maxFrame,
      const vector_size_t* frameStartsVector,
      const vector_size_t* frameEndsVector,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    SelectivityVector rows;
    rows.resize(maxFrame + 1 - minFrame);
    static auto kSingleGroup = std::vector<vector_size_t>{0};

    validRows.applyToSelected([&](auto i) {
      // Fallback path used when neither incrementalAggregation (expanding
      // frame) nor slidingAggregation (monotonic frame + retract-capable
      // aggregate) applies. Reaggregates every frame from scratch — quadratic
      // in the frame width.
      aggregate_->destroy(folly::Range<char**>(&rawSingleGroupRow_, 1));
      aggregate_->initializeNewGroups(&rawSingleGroupRow_, kSingleGroup);
      aggregateInitialized_ = true;

      auto frameStartIndex = frameStartsVector[i] - minFrame;
      auto frameEndIndex = frameEndsVector[i] - minFrame + 1;
      computeAggregate(rows, frameStartIndex, frameEndIndex);
      result->copy(aggregateResultVector_.get(), resultOffset + i, 0, 1);
    });

    // Set null values for empty (non valid) frames in the output block.
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  // Precompute and save the aggregate output for empty input in emptyResult_.
  // This value is returned for rows with empty frames.
  void computeDefaultAggregateValue(const TypePtr& resultType) {
    aggregate_->clear();
    aggregate_->initializeNewGroups(
        &rawSingleGroupRow_, std::vector<vector_size_t>{0});
    aggregateInitialized_ = true;

    emptyResult_ = BaseVector::create(resultType, 1, pool_);
    aggregate_->extractValues(&rawSingleGroupRow_, 1, &emptyResult_);
    aggregate_->clear();
  }

  // Aggregate function object required for this window function evaluation.
  std::unique_ptr<exec::Aggregate> aggregate_;

  bool aggregateInitialized_{false};

  // Current WindowPartition used for accessing rows in the apply method.
  const exec::WindowPartition* partition_;

  // Args information : their types, column indexes in inputs and vectors
  // used to populate values to pass to the aggregate function.
  // For a constant argument a column index of kConstantChannel is used in
  // argIndices_, and its ConstantVector value from the Window operator
  // is saved in argVectors_.
  std::vector<TypePtr> argTypes_;
  std::vector<column_index_t> argIndices_;
  std::vector<VectorPtr> argVectors_;

  // This is a single aggregate row needed by the aggregate function for its
  // computation. These values are for the row and its various components.
  BufferPtr singleGroupRowBufferPtr_;
  char* rawSingleGroupRow_;
  vector_size_t singleGroupRowSize_;

  // Used for per-row aggregate computations.
  // This vector is used to copy from the aggregate to the result.
  VectorPtr aggregateResultVector_;

  // Stores metadata about the previous output block of the partition
  // to optimize aggregate computation and reading argument vectors.
  std::optional<FrameMetadata> previousFrameMetadata_;

  // Stores the sliding accumulator's current contents and non-null
  // contribution count across apply() calls within a partition. Reset at
  // partition boundaries and whenever a non-sliding block is processed.
  std::optional<SlidingState> slidingState_;

  // Cached at construction so analyzeFrameValues doesn't dispatch through a
  // virtual call per block.
  bool supportsRetract_{false};

  // Stores default result value for empty frame aggregation. Window functions
  // return the default value of an aggregate (aggregation with no rows) for
  // empty frames. e.g. count for empty frames should return 0 and not null.
  VectorPtr emptyResult_;
};

} // namespace

void registerAggregateWindowFunction(const std::string& name) {
  auto aggregateFunctionSignatures = exec::getAggregateFunctionSignatures(name);
  if (aggregateFunctionSignatures.has_value()) {
    // This copy is needed to obtain a vector of the base FunctionSignaturePtr
    // from the AggregateFunctionSignaturePtr type of
    // aggregateFunctionSignatures variable.
    std::vector<exec::FunctionSignaturePtr> signatures(
        aggregateFunctionSignatures.value().begin(),
        aggregateFunctionSignatures.value().end());

    exec::registerWindowFunction(
        name,
        std::move(signatures),
        {exec::WindowFunction::ProcessMode::kRows, true},
        [name](
            const std::vector<exec::WindowFunctionArg>& args,
            const TypePtr& resultType,
            bool ignoreNulls,
            velox::memory::MemoryPool* pool,
            HashStringAllocator* stringAllocator,
            const core::QueryConfig& config)
            -> std::unique_ptr<exec::WindowFunction> {
          return std::make_unique<AggregateWindowFunction>(
              name,
              args,
              resultType,
              ignoreNulls,
              pool,
              stringAllocator,
              config);
        });
  }
}
} // namespace facebook::velox::exec
