# frozen_string_literal: true

require "helper"

class TestCommandsOnJSON < Minitest::Test
  include Helper::Client
  include Redis::Commands::JSON
  include Redis::Commands::JSON

  def test_json_set_and_get
    assert r.json_set('test', '$', { name: "John", age: 30 })
    assert_equal({ name: "John", age: 30 }, r.json_get('test'))
  end

  def test_json_mget
    r.json_set('user:1', '$', { name: "Alice", age: 25 })
    r.json_set('user:2', '$', { name: "Bob", age: 30 })
    result = r.json_mget(['user:1', 'user:2'], '$.name')
    assert_equal ["Alice", "Bob"], result
  end

  def test_json_del
    r.json_set('test', '$', { a: 1, b: 2 })
    assert_equal 1, r.json_del('test', '$.a')
    assert_equal({ b: 2 }, r.json_get('test'))
  end

  def test_json_numincrby
    r.json_set('test', '$', { num: 10 })
    assert_equal 15, r.json_numincrby('test', '$.num', 5)
  end

  def test_json_nummultby
    r.json_set('test', '$', { num: 10 })
    assert_equal 20, r.json_nummultby('test', '$.num', 2)
  end

  def test_json_strlen
    r.json_set('test', '$', { str: "Hello" })
    assert_equal [5], r.json_strlen('test', '$.str')
  end

  def test_json_arrappend
    r.json_set('test', '$', { arr: [1, 2] })
    assert_equal [4], r.json_arrappend('test', '$.arr', 3, 4)
    assert_equal [1, 2, 3, 4], r.json_get('test', '$.arr')
  end

  def test_json_arrindex
    r.json_set('test', '$', { arr: [1, 2, 3, 2] })
    assert_equal [1], r.json_arrindex('test', '$.arr', 2)
    assert_equal [3], r.json_arrindex('test', '$.arr', 2, 2)
  end

  def test_json_arrinsert
    r.json_set('test', '$', { arr: [1, 2, 4] })
    assert_equal [4], r.json_arrinsert('test', '$.arr', 2, 3)
    assert_equal [1, 2, 3, 4], r.json_get('test', '$.arr')
  end

  def test_json_arrlen
    r.json_set('test', '$', { arr: [1, 2, 3] })
    assert_equal [3], r.json_arrlen('test', '$.arr')
  end

  def test_json_arrpop
    r.json_set('test', '$', { arr: [1, 2, 3] })
    assert_equal [3], r.json_arrpop('test', '$.arr')
    assert_equal [1, 2], r.json_get('test', '$.arr')
  end

  def test_json_arrtrim
    r.json_set('test', '$', { arr: [1, 2, 3, 4, 5] })
    result = r.json_arrtrim('test', '$.arr', 1, 3)
    assert_equal [3], result
    get_result = r.json_get('test', '$.arr')
    assert_equal [2, 3, 4], get_result
  end

  def test_json_objkeys
    r.json_set('test', '$', { a: 1, b: 2, c: 3 })
    assert_equal [['a', 'b', 'c']], r.json_objkeys('test')
  end

  def test_json_objlen
    r.json_set('test', '$', { a: 1, b: 2, c: 3 })
    assert_equal [3], r.json_objlen('test')
  end

  def test_json_set_with_options
    assert r.json_set('test', '$', { name: "John", age: 30 })
    assert_nil r.json_set('test', '$', { name: "Jane" }, nx: true)
    assert r.json_set('test', '$', { name: "Jane" }, xx: true)
    assert_equal({ name: "Jane" }, r.json_get('test'))
  end

  def test_json_set_update_specific_field
    assert r.json_set('test', '$', { name: "John", age: 30 })
    assert r.json_set('test', '$.age', 31)
    assert_equal({ name: "John", age: 31 }, r.json_get('test'))
  end

  def test_json_set_add_new_field
    assert r.json_set('test', '$', { name: "John" })
    assert r.json_set('test', '$.age', 30)
    assert_equal({ name: "John", age: 30 }, r.json_get('test'))
  end

  def test_json_mget_complex
    r.json_set('user:1', '$', { name: "Alice", age: 25, address: { city: "New York" } })
    r.json_set('user:2', '$', { name: "Bob", age: 30, address: { city: "San Francisco" } })
    result = r.json_mget(['user:1', 'user:2'], '$.address.city')
    assert_equal ["New York", "San Francisco"], result
  end

  def test_json_type_nested
    r.json_set('test', '$', { a: 1, b: { c: "string", d: [1, 2, 3] } })
    assert_equal ['integer'], r.json_type('test', '$.a')
    assert_equal ['object'], r.json_type('test', '$.b')
    assert_equal ['string'], r.json_type('test', '$.b.c')
    assert_equal ['array'], r.json_type('test', '$.b.d')
  end

  def test_json_numincrby_float
    r.json_set('test', '$', { num: 10.5 })
    assert_equal 13.7, r.json_numincrby('test', '$.num', 3.2)
  end

  def test_json_nummultby_float
    r.json_set('test', '$', { num: 10.5 })
    assert_equal 26.25, r.json_nummultby('test', '$.num', 2.5)
  end

  def test_json_arrappend_multiple_values
    r.json_set('test', '$', { arr: [1, 2] })
    assert_equal [5], r.json_arrappend('test', '$.arr', 3, 4, 5)
    assert_equal [1, 2, 3, 4, 5], r.json_get('test', '$.arr')
  end

  def test_json_arrindex_with_range
    r.json_set('test', '$', { arr: [1, 2, 3, 2, 4, 2] })
    assert_equal [1], r.json_arrindex('test', '$.arr', 2)
    assert_equal [3], r.json_arrindex('test', '$.arr', 2, 2)
    assert_equal [5], r.json_arrindex('test', '$.arr', 2, 4)
    assert_equal [5], r.json_arrindex('test', '$.arr', 2, 6)
  end

  def test_json_arrinsert_multiple_values
    r.json_set('test', '$', { arr: [1, 2, 5] })
    assert_equal [5], r.json_arrinsert('test', '$.arr', 2, 3, 4)
    assert_equal [1, 2, 3, 4, 5], r.json_get('test', '$.arr')
  end

  def test_json_arrpop_with_index
    r.json_set('test', '$', { arr: [1, 2, 3, 4, 5] })
    assert_equal [3], r.json_arrpop('test', '$.arr', 2)
    assert_equal [1, 2, 4, 5], r.json_get('test', '$.arr')
    assert_equal [1], r.json_arrpop('test', '$.arr', 0)
    assert_equal [2, 4, 5], r.json_get('test', '$.arr')
    assert_equal [5], r.json_arrpop('test', '$.arr', -1)
    assert_equal [2, 4], r.json_get('test', '$.arr')
  end

  def test_json_operations_on_nested_arrays
    r.json_set('test', '$', { nested: { arr: [1, [2, 3], 4] } })
    assert_equal [3], r.json_arrlen('test', '$.nested.arr')
    assert_equal [2], r.json_arrlen('test', '$.nested.arr[1]')
    assert_equal [3], r.json_arrappend('test', '$.nested.arr[1]', 4)
    assert_equal [1, [2, 3, 4], 4], r.json_get('test', '$.nested.arr')
    assert_equal [1], r.json_arrindex('test', '$.nested.arr', [2, 3, 4])
    r.json_arrinsert('test', '$.nested.arr', 1, 'inserted')
    assert_equal [1, 'inserted', [2, 3, 4], 4], r.json_get('test', '$.nested.arr')
  end

  def test_json_set_and_get_with_path
    r.json_set('test', '$', { user: { name: "John", address: { city: "New York" } } })
    assert_equal "John", r.json_get('test', '$.user.name')
    assert_equal "New York", r.json_get('test', '$.user.address.city')
    r.json_set('test', '$.user.address.country', "USA")
    assert_equal({ city: "New York", country: "USA" }, r.json_get('test', '$.user.address'))
  end

  def test_json_type_with_complex_structure
    r.json_set('test', '$', {
                 null_value: nil,
                 string_value: "hello",
                 number_value: 42,
                 float_value: 3.14,
                 boolean_value: true,
                 array_value: [1, 2, 3],
                 object_value: { key: "value" }
               })
    assert_equal ['null'], r.json_type('test', '$.null_value')
    assert_equal ['string'], r.json_type('test', '$.string_value')
    assert_equal ['integer'], r.json_type('test', '$.number_value')
    assert_equal ['number'], r.json_type('test', '$.float_value')
    assert_equal ['boolean'], r.json_type('test', '$.boolean_value')
    assert_equal ['array'], r.json_type('test', '$.array_value')
    assert_equal ['object'], r.json_type('test', '$.object_value')
  end

  def test_json_strappend_with_nested_path
    r.json_set('test', '$', { user: { name: "John" } })
    assert_equal [8], r.json_strappend('test', '$.user.name', " Doe")
    assert_equal "John Doe", r.json_get('test', '$.user.name')
  end

  def test_json_numincrby_and_nummultby_with_nested_path
    r.json_set('test', '$', { user: { stats: { points: 100, multiplier: 2 } } })
    assert_equal 150, r.json_numincrby('test', '$.user.stats.points', 50)
    assert_equal 300, r.json_nummultby('test', '$.user.stats.points', 2)
    assert_equal 6, r.json_nummultby('test', '$.user.stats.multiplier', 3)
  end

  def test_json_del_with_nested_path
    r.json_set('test', '$', { user: { name: "John", age: 30, address: { city: "New York", country: "USA" } } })
    assert_equal 1, r.json_del('test', '$.user.age')
    assert_equal 1, r.json_del('test', '$.user.address.city')
    expected = { user: { name: "John", address: { country: "USA" } } }
    assert_equal expected, r.json_get('test')
  end

  def test_json_arrpop_empty_array
    r.json_set('test', '$', { arr: [] })
    assert_equal [nil], r.json_arrpop('test', '$.arr')
    assert_equal [], r.json_get('test', '$.arr')
  end

  def test_json_arrindex_non_existent_value
    r.json_set('test', '$', { arr: [1, 2, 3] })
    assert_equal([-1], r.json_arrindex('test', '$.arr', 4))
  end

  def test_json_object_operations
    r.json_set('test', '$', { user: { name: "John", age: 30 } })
    assert_equal [['name', 'age']], r.json_objkeys('test', '$.user')
    assert_equal [2], r.json_objlen('test', '$.user')
    r.json_set('test', '$.user.email', 'john@example.com')
    assert_equal [3], r.json_objlen('test', '$.user')
    assert_includes r.json_objkeys('test', '$.user').first, 'email'
  end

  def test_json_operations_on_non_existent_key
    assert_nil r.json_get('non_existent')
    assert_equal 0, r.json_del('non_existent')
    assert_nil r.json_type('non_existent')

    error_message = "ERR could not perform this operation on a key that doesn't exist"
    assert_raises(Redis::CommandError, error_message) do
      r.json_strappend('non_existent', '$', 'append_me')
    end

    assert_raises(Redis::CommandError, error_message) do
      r.json_arrappend('non_existent', '$', 1)
    end
  end

  def test_json_set_with_large_nested_structure
    large_structure = {
      level1: {
        level2: {
          level3: {
            level4: {
              level5: {
                data: "Deep nested data",
                array: [1, 2, 3, 4, 5],
                nested_object: {
                  key1: "value1",
                  key2: "value2"
                }
              }
            }
          }
        }
      }
    }
    assert r.json_set('test', '$', large_structure)
    assert_equal "Deep nested data", r.json_get('test', '$.level1.level2.level3.level4.level5.data')
    assert_equal [1, 2, 3, 4, 5], r.json_get('test', '$.level1.level2.level3.level4.level5.array')
    assert_equal "value2", r.json_get('test', '$.level1.level2.level3.level4.level5.nested_object.key2')
  end

  def test_jsonset_jsonget_mixed_types
    d = { hello: "world", some: "value" }
    assert r.json_set("somekey", "$", d)
    assert_equal d, r.json_get("somekey")
  end

  def test_nonascii_setgetdelete
    assert r.json_set("notascii", "$", "hyvää-élève")
    assert_equal "hyvää-élève", r.json_get("notascii")
    assert_equal 1, r.json_del("notascii")
    assert_equal 0, r.exists("notascii")
  end

  def test_jsonsetexistentialmodifiersshouldsucceed
    obj = { "foo" => "bar" }
    assert r.json_set("obj", "$", obj)

    # Test that flags prevent updates when conditions are unmet
    assert_nil r.json_set("obj", "$.foo", "baz", nx: true)
    assert_nil r.json_set("obj", "$.qaz", "baz", xx: true)

    # Test that flags allow updates when conditions are met
    assert r.json_set("obj", "$.foo", "baz", xx: true)
    assert r.json_set("obj", "$.qaz", "baz", nx: true)

    # Test that flags are mutually exclusive
    assert_raises(Redis::CommandError) do
      r.json_set("obj", "$.foo", "baz", nx: true, xx: true)
    end
  end

  def test_mget
    r.json_set("1", "$", 1)
    r.json_set("2", "$", 2)
    assert_equal [1], r.json_mget(["1"], "$")
    assert_equal [1, 2], r.json_mget(["1", "2"], "$")
  end

  def test_json_mset
    triplets = [
      ["key1", "$", { name: "John", age: 30 }],
      ["key2", "$", { name: "Jane", age: 25 }]
    ]
    assert_equal "OK", r.json_mset(triplets)

    assert_equal({ name: "John", age: 30 }, r.json_get("key1"))
    assert_equal({ name: "Jane", age: 25 }, r.json_get("key2"))

    assert_equal [{ name: "John", age: 30 }, { name: "Jane", age: 25 }], r.json_mget(["key1", "key2"], "$")
  end

  def test_json_arrappend_and_arrlen
    r.json_set('arr', '$', [1, 2])
    assert_equal [4], r.json_arrappend('arr', '$', 3, 4)
    assert_equal [4], r.json_arrlen('arr', '$')
    assert_equal [1, 2, 3, 4], r.json_get('arr')
  end

  def test_json_numincrby_and_nummultby
    r.json_set('num', '$', { "value": 10 })
    assert_equal 15, r.json_numincrby('num', '$.value', 5)
    assert_equal 30, r.json_nummultby('num', '$.value', 2)
    assert_equal({ value: 30 }, r.json_get('num'))
  end

  def test_json_objkeys_and_objlen
    r.json_set('obj', '$', { "name" => "John", "age" => 30, "city" => "New York" })
    assert_equal [['name', 'age', 'city']], r.json_objkeys('obj', '$').sort
    assert_equal [3], r.json_objlen('obj', '$')
  end

  def test_json_strappend_and_strlen
    r.json_set('str', '$', "Hello")
    assert_equal [11], r.json_strappend('str', '$', " World")
    assert_equal [11], r.json_strlen('str', '$')
    assert_equal "Hello World", r.json_get('str')
  end

  def test_json_toggle
    r.json_set('toggle', '$', { "flag" => false })
    assert_equal [1], r.json_toggle('toggle', '$.flag')
    assert_equal [0], r.json_toggle('toggle', '$.flag')
    assert_equal({ flag: false }, r.json_get('toggle'))
  end

  def test_json_clear
    r.json_set('clear', '$', { "arr" => [1, 2, 3], "obj" => { "a" => 1, "b" => 2 } })
    result = r.json_clear('clear', '$')
    assert_equal 1, result
    assert_equal({}, r.json_get('clear'))
  end

  def test_json_mget_with_complex_paths
    r.json_set('user1', '$', { name: "John", age: 30, pets: ["dog", "cat"] })
    r.json_set('user2', '$', { name: "Jane", age: 28, pets: ["fish"] })

    result = r.json_mget(['user1', 'user2'], '$')
    expected = [
      { name: "John", age: 30, pets: ["dog", "cat"] },
      { name: "Jane", age: 28, pets: ["fish"] }
    ]
    assert_equal expected, result
  end

  def test_json_strappend_single_path
    r.json_set('obj', '$', { str1: "Hello", str2: "World" })
    result = r.json_strappend('obj', '$.str1', " Redis")
    assert_equal [11], result
    assert_equal({ str1: "Hello Redis", str2: "World" }, r.json_get('obj'))
  end

  def test_json_arrpop_single_path
    r.json_set('obj', '$', {
                 arr1: [1, 2, 3],
                 arr2: [4, 5, 6],
                 not_arr: "string"
               })
    result = r.json_arrpop('obj', '$.arr1', -1)
    assert_equal [3], result
    assert_equal({
                   arr1: [1, 2],
                   arr2: [4, 5, 6],
                   not_arr: "string"
                 }, r.json_get('obj'))
  end

  def test_json_resp
    r.json_set('resp', '$', { 'foo': 'bar', 'baz': 42, 'qux': true })
    result = r.json_resp('resp', '$')
    assert_equal [["{", "foo", "bar", "baz", 42, "qux", "true"]], result
  end

  def test_json_debug_memory
    r.json_set('debug', '$', { 'foo': 'bar', 'baz': [1, 2, 3] })
    memory_usage = r.json_debug('MEMORY', 'debug', '$')
    assert_kind_of Integer, memory_usage.first
    assert memory_usage.first > 0
  end

  def test_json_get_complex_path
    r.json_set('complex', '$', {
                 'users': [
                   { 'name': 'John', 'age': 30 },
                   { 'name': 'Jane', 'age': 25 }
                 ],
                 'products': [
                   { 'name': 'Apple', 'price': 1.0 },
                   { 'name': 'Banana', 'price': 0.5 }
                 ]
               })
    result = r.json_get('complex', '$.users[?(@.age>28)].name')
    assert_equal 'John', result
  end

  def test_json_operations_on_non_existent_paths
    r.json_set('nested', '$', { 'a': { 'b': 1 } })
    arrappend_result = r.json_arrappend('nested', '$.nonexistent', 1)

    assert_equal [], arrappend_result
  end

  def test_json_arrindex_out_of_range
    r.json_set('arr', '$', [1, 2, 3, 4, 5])
    assert_equal([-1], r.json_arrindex('arr', '$', 6))
    assert_equal([-1], r.json_arrindex('arr', '$', 3, 10))
  end

  def test_json_arrappend_behavior
    r.json_set('test', '$', { 'existing': [1, 2, 3] })
    # Returns nil for non-existent paths
    assert_equal [], r.json_arrappend('test', '$.nonexistent', 4)
    # Returns the new length for existing paths
    assert_equal [4], r.json_arrappend('test', '$.existing', 4)
    # Verify the actual state after operations
    assert_equal({ existing: [1, 2, 3, 4] }, r.json_get('test'))
  end

  def test_json_type_all_types
    r.json_set('test', '$', {
                 null: nil,
                 bool: true,
                 int: 42,
                 float: 3.14,
                 string: 'hello',
                 array: [1, 2, 3],
                 object: { a: 1 }
               })

    assert_equal ['null'], r.json_type('test', '$.null')
    assert_equal ['boolean'], r.json_type('test', '$.bool')
    assert_equal ['integer'], r.json_type('test', '$.int')
    assert_equal ['number'], r.json_type('test', '$.float')
    assert_equal ['string'], r.json_type('test', '$.string')
    assert_equal ['array'], r.json_type('test', '$.array')
    assert_equal ['object'], r.json_type('test', '$.object')
  end

  def test_json_strappend_multiple_paths
    r.json_set('test', '$', { a: 'Hello', b: 'World' })
    result = r.json_strappend('test', '$.*', ' Redis')
    assert_equal [11, 11], result
    assert_equal({ a: 'Hello Redis', b: 'World Redis' }, r.json_get('test'))
  end

  def test_json_numincrby_multiple_paths
    r.json_set('test', '$', { a: 1, b: 2, c: 3 })
    result = r.json_numincrby('test', '$.*', 10)
    assert_equal [11, 12, 13], result
    assert_equal({ a: 11, b: 12, c: 13 }, r.json_get('test'))
  end

  def test_json_toggle_multiple_paths
    r.json_set('test', '$', { a: true, b: false, c: true })
    result = r.json_toggle('test', '$.*')
    assert_equal [0, 1, 0], result
    assert_equal({ a: false, b: true, c: false }, r.json_get('test'))
  end

  def test_json_merge
    # Test with root path $
    r.json_set("person_data", "$", { person1: { personal_data: { name: "John" } } })
    r.json_merge("person_data", "$", { person1: { personal_data: { hobbies: "reading" } } })
    assert_equal({ person1: { personal_data: { name: "John", hobbies: "reading" } } }, r.json_get("person_data"))

    # Test with root path $.person1.personal_data
    r.json_merge("person_data", "$.person1.personal_data", { country: "Israel" })
    assert_equal({ person1: { personal_data: { name: "John", hobbies: "reading", country: "Israel" } } }, r.json_get("person_data"))

    # Test with null value to delete a value
    r.json_merge("person_data", "$.person1.personal_data", { name: nil })
    assert_equal({ person1: { personal_data: { country: "Israel", hobbies: "reading" } } }, r.json_get("person_data"))
  end
end
