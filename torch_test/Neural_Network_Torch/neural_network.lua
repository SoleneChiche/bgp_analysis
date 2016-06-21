-- First, be sure to require the 'nn' package for the neural network functions
require 'nn';

-- Number of training samples (pair {600'000 input, 600'000 output expected})
nExamples = 10
-- Input size: size of the largest burst found in the training set
-- Contain the prefixes observed
inputSize = 600000
-- Output size: same size as input.
-- Contain the correct next prefixes to be predicted
outputSize = 600000

-- Trainset empty, to be filled with the csv files
trainset = {}
trainset.data = torch.Tensor(nExamples, inputSize):zero()
trainset.label = torch.Tensor(nExamples, outputSize):zero()

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

-- Generate the Training set from the csv files
function GenerateTrainingSet()
    for i=1,nExamples do
      for j=1,inputSize do
        -- Data = input for the system
        trainset.data[i][j] = 0
        -- Label = output expected as groundtruth
        -- TODO: label will need normalization
        trainset.label[i][j] = 1
      end
    end
--    print(trainset.data)
--    print(trainset.label)
end


GenerateTrainingSet()

-- This is where we build the model
model = nn.Sequential()                       -- Create a Sequential network
-- First Linear layer (TODO: SparseLayer instead)
model:add(nn.Linear(inputSize, 100000))
-- Current most effective activation function
model:add(nn.ReLU())
-- Second Linear layer (TODO: SparseLayer instead)
model:add(nn.Linear(100000,outputSize))
-- Current most effective activation function
model:add(nn.SoftMax())

print(model)

criterion = nn.MSECriterion()
trainer = nn.StochasticGradient(model, criterion)
trainer.learningRate = 0.01
trainer.maxIteration = 100

trainer:train(trainset)

a = torch.Tensor(inputSize):zero()
for k=1, inputSize do
  a[k] = math.random(0, 1)
end
b = model:forward(a)
print(b)
