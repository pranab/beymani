

class IdGenerator

	def initialize
		@id_char = ['0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O',
		'P','Q','R','S','T','U','V','W','X','Y','Z', '0', '1', '2', '3', '4']
		@length = @id_char.length
		@id_char_num = ['0','1','2','3','4','5','6','7','8','9']
		
	end 
	
	def generate(length)
		id = ''
		1.upto length do 
			id << @id_char[rand(@length)]
		end
		return id
	end
	
	def genAndStore(count, length)
		@ids = []
		1.upto count do
			@ids << generate(length)
		end
	end

	def generateNum(length)
		id = ''
		1.upto length do 
			id << @id_char_num[rand(@id_char_num.length)]
		end
		return id
	end
	
	def genAndStoreNum(count, length)
		@ids = []
		1.upto count do
			@ids << generateNum(length)
		end
	end

	def sample
		@ids[rand(@ids.size)]
	end

end

class NameGenerator

	def initialize
		@name_char = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s',
			't','u','v','w','x','y','z']
		@length = @name_char.length
	end 
	
	def generate(length)
		name = ''
		1.upto length do 
			name << @name_char[rand(@length)]
		end
		return name
	end
	
	def genAndStore(count, *lengths)
		@names = []
		1.upto count do
			@names << generate(lengths[rand(lengths.size)])
		end
	end
	
	def sample
		@names[rand(@names.size)]
	end

end

class EvenGenerator
	def addEvent(event, *prob)
		@events[event] = prob
	end
	
	def generate
		evtGen = {}
		
		#independent
		@events.each do |k, v|
			if (v.length == 1)
				evtGen[k] = v[0] < rand(100)
			end
		end
		
		#conditional
		@events.each do |k, v|
			if (v.length > 1)
				condEvt = v[1]
				if (evtGen[condEvt])
					evtGen[k] = v[2] < rand(100)
				else 
					evtGen[k] = v[0] < rand(100)
				end
			end
		end
		
	end
	
end

class IdField
	def initialize(length, idGen)
		@length = length
		@idGen = idGen
	end
	
	def value
		@idGen.generate(@length)
	end
	
end

class CategoricalField

	def initialize(*args)
		@dist = []
		values = args.size == 1 ? args[0] : args
		i = 0;
		while (i < values.length)
			val = values[i]
			count = values[i + 1]
			1.upto count do
				@dist << val
			end
			i = i + 2
		end
	end

	def value 
		@dist[rand(@dist.length)]
	end
	
end

class NumericalField
	def initialize(uniform, *args)
		@uniform = uniform
		@dist = []
		
		if (uniform)
			@min = args[0]
			@max = args[1]
		else 
			values = args.size == 1 ? args[0] : args
			i = 0;
			while (i < values.length)
				val = values[i]
				count = values[i + 1]
				
				j = 0
				done = false
				while (!done)
					val.each do |v|
						@dist << v
						j = j + 1
						if (j == count) 
							done = true
							break
						end
					end
				end
				i = i + 2
			end
		end
	end

	def value 
		if (@uniform)
			@min + rand(@max - @min)
		else 
			@dist[rand(@dist.length)]
		end
	end
end

