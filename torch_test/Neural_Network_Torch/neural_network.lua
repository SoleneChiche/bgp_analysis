-- First, be sure to require the 'nn' package for the neural network functions
require 'nn';
require "nnsparse";
csv2tensor = require 'csv2tensor'

-- Number of training samples
nExamples = 50
-- Input size: all prefixes observed
inputSize = 759778
-- Output size: same size as input.
outputSize = 759778

-- This is where we build the model
model = nn.Sequential()                       -- Create a Sequential network
-- First SparseLinear layer
model:add(nn.SparseLinear(inputSize, 1000))
-- Current most effective activation function
model:add(nn.ReLU())
-- Second SparseLinear layer
model:add(nn.Linear(1000, outputSize))
--model:add(nn.ReLU())
-- Current most effective activation function
--model:add(nn.SoftMax())

print(model)

criterion = nn.AbsCriterion()
--nn.MultiLabelSoftMarginCriterion() --nn.MSECriterion() --nn.ClassNLLCriterion() --nnsparse.SparseCriterion(nn.MSECriterion())
trainer = nn.StochasticGradient(model, criterion)
trainer.learningRate = 0.1
trainer.maxIteration = 50

trainset = {}

--The network trainer expects an index metatable
setmetatable(trainset,
{__index = function(t, i)
    return {t.data[i], t.label[i]}  -- The trainer is expecting trainset[123] to be {data[123], label[123]}
    end}
);

--The network trainer expects a size function
function trainset:size()
    return self.data:size(1)
end

function GenerateTrainingSet()
    trainset.data = torch.Tensor(nExamples,200,2):zero()
    trainset.label = torch.Tensor(nExamples, outputSize):zero()

    for i=1,nExamples do
        local start = (i-1) * 210
        local mid = start + 200
        local stop = mid + 10

        -- Generate the Training set from the csv files
        local oneDTensorInput, colIn = csv2tensor.load('burstInput/inputVector'..tostring(start)..'-'..tostring(mid)..'.csv')
        local twoDTensorInput = oneDTensorInput:sparsify()

        local oneDTensorLabel, colOut = csv2tensor.load('burstLabel/labelVector'..tostring(mid)..'-'..tostring(stop)..'.csv')

        trainset.data[i] = twoDTensorInput
        trainset.label[i] = oneDTensorLabel-- / torch.norm(oneDTensorLabel)
    end
end

oneDTensorInput, column_names_input = csv2tensor.load('burstInput/inputVector0-200.csv')
twoDTensorInput = oneDTensorInput:sparsify()
--oneDTensorLabel, column_names_label = csv2tensor.load('burstLabel/labelVector200-210.csv')
--pairInputLabel = {twoDTensorInput, oneDTensorLabel / torch.norm(oneDTensorLabel)}
--print(pairInputLabel)
--trainset.data = torch.reshape(twoDTensorInput, nExamples, 200, 2)
--trainset.label = torch.reshape(oneDTensorLabel / torch.norm(oneDTensorLabel), nExamples, outputSize)


print("Generating training set...")
GenerateTrainingSet()
print("Train the model...")
trainer:train(trainset)
print("Forward an input to be tested")
print(model:forward(twoDTensorInput))

